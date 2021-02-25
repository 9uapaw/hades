from typing import List, Type

from core.cmd import RunnableCommand, RemoteRunnableCommand
from core.config import ClusterConfig, ClusterContextConfig, ClusterRoleConfig
from core.context import HadesContext
from core.error import HadesException
from hadoop.app.example import ApplicationCommand
from hadoop.cluster_type import ClusterType
from hadoop.cm.cm_api import CmApi
from hadoop.config import HadoopConfig
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.host import HadoopHostInstance, RemoteHostInstance
from hadoop.role import HadoopRoleInstance, HadoopRoleType


class CmExecutor(HadoopOperationExecutor):
    ROLE_NAME_MAP = {"jobhistory": "job-historyserver"}
    LOG_DIR = "/var/log/"

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

        except Exception as e:
            raise HadesException("Error while reading CM api", e)

        return cluster

    def read_log(self, *args: HadoopRoleInstance, follow: bool = False, tail: int or None = 10) -> List[RunnableCommand]:
        cmds = []
        for role in args:
            role_type = role.role_type.value
            for k, v in self.ROLE_NAME_MAP.items():
                if v == role.role_type.value:
                    role_type = k

            file = "{log_dir}*/*{role_type}*".format(log_dir=self.LOG_DIR, role_type=role_type.upper())
            if tail or follow:
                cmd = "tail -f {file}"
            else:
                cmd = "cat {file}"
            cmds.append(role.host.run_cmd(cmd.format(file=file)))

        return cmds

    def get_cluster_status(self, cluster_name: str = None) -> List[HadoopClusterStatusEntry]:
        services = self._cm_api.get_services(cluster_name)
        statuses = []

        for service in services:
            roles = self._cm_api.get_roles(cluster_name, service.name)
            for role in roles:
                statuses.append(HadoopClusterStatusEntry(role.name, role.health_summary))

        return statuses

    def run_app(self, random_selected: HadoopRoleInstance, application: ApplicationCommand):
        application.path = "/opt/cloudera/parcels/CDH/jars"
        cmd = "{} {}".format(self._ctx.config.cmd_prefix,
                             application.build()) if self._ctx.config.cmd_prefix else application.build()
        cmd = random_selected.host.run_cmd(cmd)
        cmd.run_async()

    def update_config(self, *args: HadoopRoleInstance, config: HadoopConfig, no_backup: bool):
        pass

    def restart_roles(self, *args: HadoopRoleInstance):
        pass
