import logging
from pathlib import PurePath
from typing import List, Type, Dict

from cm_client.rest import ApiException

from core.cmd import RunnableCommand, RemoteRunnableCommand
from core.config import ClusterConfig, ClusterContextConfig, ClusterRoleConfig
from core.context import HadesContext
from core.error import HadesException, CommandExecutionException
from hadoop.app.example import ApplicationCommand
from hadoop.cluster_type import ClusterType
from hadoop.cm.cm_api import CmApi
from hadoop.config import HadoopConfig
from hadoop.data.status import HadoopClusterStatusEntry, HadoopConfigEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.host import HadoopHostInstance, RemoteHostInstance
from hadoop.role import HadoopRoleInstance, HadoopRoleType
from hadoop.xml_config import HadoopConfigFile
from hadoop_dir.module import HadoopModule, HadoopDir


logger = logging.getLogger(__name__)


class CmExecutor(HadoopOperationExecutor):
    ROLE_NAME_MAP = {"jobhistory": "job-historyserver"}
    LOG_DIR = "/var/log/"
    PROCESS_DIR = "/run/cloudera-scm-agent/process/"
    JAR_DIR = "/opt/cloudera/parcels/CDH/jars/"

    def __init__(self, ctx: HadesContext, cm_api: CmApi):
        self._cm_api = cm_api
        self._ctx = ctx

    @property
    def role_host_type(self) -> Type[HadoopHostInstance]:
        return RemoteHostInstance

    def discover(self) -> ClusterConfig:
        cluster = ClusterConfig(ClusterType.CM)
        try:
            clusters_from_api = self._cm_api.get_clusters()
            cluster.cluster_name = clusters_from_api[0].name
            services_from_api = self._cm_api.get_services(clusters_from_api[0].name)

            for service in services_from_api:
                context_config = ClusterContextConfig()
                if service.type == "HDFS":
                    cluster.context['Hdfs'] = context_config
                elif service.type == "YARN":
                    cluster.context['Yarn'] = context_config
                else:
                    continue

                context_config.name = service.name
                roles_from_api = self._cm_api.get_roles(clusters_from_api[0].name, service.name)
                for role in roles_from_api:  # type: str, dict
                    role_type = role.type.lower()
                    role_name = role.name
                    role_conf = ClusterRoleConfig()
                    try:
                        role_conf.type = HadoopRoleType(self.ROLE_NAME_MAP.get(role_type, role_type))
                    except ValueError:
                        continue
                    role_conf.host = role.host_ref.hostname
                    context_config.roles[role_name] = role_conf

        except ApiException as e:
            raise HadesException("Error while reading CM api", e.reason)

        return cluster

    def read_log(self, *args: HadoopRoleInstance, follow: bool = False, tail: int or None = 10, download: bool = True) -> List[RunnableCommand]:
        cmds = []
        for role in args:
            role_type = role.role_type.value
            for k, v in self.ROLE_NAME_MAP.items():
                if v == role.role_type.value:
                    role_type = k

            file = "{log_dir}*/*{role_type}*".format(log_dir=self.LOG_DIR, role_type=role_type.upper())
            if download:
                cmds.append(role.host.download(file))
                continue
            elif tail or follow:
                cmd = "tail -f {file}"
            else:
                cmd = "cat {file}"
            cmds.append(role.host.create_cmd(cmd.format(file=file)))

        return cmds

    def get_cluster_status(self, cluster_name: str = None) -> List[HadoopClusterStatusEntry]:
        services = self._cm_api.get_services(cluster_name)
        statuses = []

        for service in services:
            roles = self._cm_api.get_roles(cluster_name, service.name)
            for role in roles:
                statuses.append(HadoopClusterStatusEntry(role.name, role.health_summary))

        return statuses

    def run_app(self, random_selected: HadoopRoleInstance, application: ApplicationCommand) -> RunnableCommand:
        application.path = "/opt/cloudera/parcels/CDH/jars"
        cmd = random_selected.host.create_cmd(application.build())

        return cmd

    def update_config(self, *args: HadoopRoleInstance, config: HadoopConfig, no_backup: bool, workdir: str = "."):
        for role in args:
            existing_conf = self._cm_api.get_config(role.service.cluster.name, role.name, role.service.name)
            safety_valve = ""
            for ex in existing_conf:
                if ex.name == "resourcemanager_capacity_scheduler_configuration":
                    safety_valve = ex.value
            c = {}
            if role.role_type == HadoopRoleType.RM:
                config.set_xml_str(safety_valve)
                config.merge()
                c["resourcemanager_capacity_scheduler_configuration"] = config.to_str()

            self._cm_api.update_config(role.service.cluster.name, role.name, role.service.name, c)

    def get_config(self, *args: HadoopRoleInstance, config: HadoopConfigFile) -> Dict[str, HadoopConfig]:
        find_config = "find {} -name \"*{}*\" -print".format(self.PROCESS_DIR, config.value)
        configs = {}
        for role in args:
            try:
                config_paths = role.host.create_cmd(find_config).run()
            except CommandExecutionException as e:
                if e.stdout:
                    config_paths = e.stdout
                else:
                    raise e
            recent_process = [path for path in config_paths[0] if "process" in path]
            if recent_process:
                recent_process = recent_process[0]
            else:
                continue
            xml = role.host.create_cmd("cat {}".format(recent_process)).run()
            config = HadoopConfig(config)
            config.set_xml_str("\n".join(xml[0]))
            configs[role.name] = config

        return configs

    def replace_module_jars(self, *args: HadoopRoleInstance, modules: HadoopDir):
        unique_args = {role.host.get_address(): role for role in args}
        cached_found_jar = {}
        for role in unique_args.values():
            logger.info("Replacing jars on {}".format(role.host.get_address()))
            for module, jar in modules.get_jar_paths().items():
                logger.info("Replacing jar {}".format(jar))
                local_jar = jar
                if module not in cached_found_jar:
                    find_remote_jar = role.host.find_file(self.JAR_DIR, "*{}*".format(module)).run()
                    remote_jar = ""
                    if find_remote_jar[0]:
                        remote_jar = find_remote_jar[0][0]
                        cached_found_jar[module] = remote_jar
                else:
                    remote_jar = cached_found_jar[module]

                if remote_jar:
                    role.host.make_backup(remote_jar).run()
                    role.host.upload(local_jar, remote_jar).run()

    def restart_roles(self, *args: HadoopRoleInstance):
        roles_by_services: Dict[str, List[HadoopRoleInstance]] = {}
        for role in args:
            if role.service.name in roles_by_services:
                roles_by_services[role.service.name].append(role)
            else:
                roles_by_services[role.service.name] = [role]

        for service_name, roles in roles_by_services.items():
            self._cm_api.restart_roles(roles[0].service.cluster.name, service_name, *[role.name for role in roles])

        return []

    def restart_cluster(self, cluster: str):
        self._cm_api.restart_cluster(cluster)
