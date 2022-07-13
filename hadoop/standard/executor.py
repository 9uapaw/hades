import logging
import time
import os
from typing import List, Type, Dict

from core.cmd import RunnableCommand
from core.config import ClusterConfig, ClusterRoleConfig, ClusterContextConfig
from hadoop.app.example import ApplicationCommand, MapReduceApp, DistributedShellApp
from hadoop.cluster_type import ClusterType
from hadoop.config import HadoopConfig
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.host import HadoopHostInstance, RemoteHostInstance
from hadoop.role import HadoopRoleType, HadoopRoleInstance
from hadoop.xml_config import HadoopConfigFile
from hadoop.yarn.rm_api import RmApi
from hadoop_dir.module import HadoopDir


logger = logging.getLogger(__name__)


class StandardUpstreamExecutor(HadoopOperationExecutor):
    JAR_DIR = "/opt/hadoop/share/"
    LOG_DIR = "/opt/hadoop/logs"
    CONFIG_FILE_PATH = "/opt/hadoop/etc/hadoop/{}"

    def __init__(self, rm_host: str):
        self._rm_host = rm_host

    @property
    def role_host_type(self) -> Type[HadoopHostInstance]:
        return RemoteHostInstance

    def discover(self) -> ClusterConfig:
        cluster = ClusterConfig(ClusterType.STANDARD.value)
        yarn_roles = {}
        hdfs_roles = {}
        role_instance = HadoopRoleInstance(None, "resourcemanager", None, None)
        rm_host = HadoopHostInstance(role_instance, self._rm_host, "yarn")
        role_instance.host = rm_host
        rm_api = RmApi(role_instance)

        rm_host_id = self._rm_host.replace("http://", "")
        rm_role_conf = ClusterRoleConfig()
        rm_role_conf.type = HadoopRoleType.RM
        rm_role_conf.host = rm_host_id
        yarn_roles[rm_host_id] = rm_role_conf

        for node in rm_api.get_nodes():
            role_conf = ClusterRoleConfig()
            role_conf.type = HadoopRoleType.NM
            role_conf.host = node['nodeHostName']
            yarn_roles[node['id']] = role_conf

        cluster.context['Yarn'] = ClusterContextConfig()
        cluster.context['Hdfs'] = ClusterContextConfig()
        cluster.context['Yarn'].name = 'Yarn'
        cluster.context['Yarn'].roles = yarn_roles
        cluster.context['Hdfs'].name = 'Hdfs'
        cluster.context['Hdfs'].roles = hdfs_roles

        return cluster

    def read_log(self, *args: HadoopRoleInstance, follow: bool = False, tail: int or None = 10, download: bool = None) -> List[RunnableCommand]:
        cmds = []
        for role in args:
            role_type = role.role_type.value

            file = "{log_dir}*/*{role_type}*".format(log_dir=self.LOG_DIR, role_type=role_type)
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
        raise NotImplementedError()

    def run_app(self, random_selected: HadoopRoleInstance, application: ApplicationCommand):
        application.path = "/opt/hadoop/share/hadoop/"
        if isinstance(application, MapReduceApp):
            application.path += "mapreduce"
        elif isinstance(application, DistributedShellApp):
            application.path += "yarn"

        application_command = application.build()
        full_command = "/opt/hadoop/bin/{}".format(application_command)
        cmd = random_selected.host.create_cmd(full_command)

        return cmd

    def update_config(self, *args: HadoopRoleInstance, config: HadoopConfig, no_backup: bool = False):
        config_name, config_ext = config.file.split(".")

        for role in args:
            logger.info("Setting config {} on {}".format(config.file, role.get_colorized_output()))
            local_file = "{config}-{host}-{time}.{ext}".format(
                host=role.host, config=config_name, time=int(time.time()), ext=config_ext)
            config_file_path = self.CONFIG_FILE_PATH.format(config.file)
            role.host.download(config_file_path, local_file).run()

            config.xml = local_file
            config.merge()
            config.commit()

            role.host.upload(config.file, config_file_path).run()

            if no_backup:
                logger.info("Backup is turned off. Deleting file {}".format(local_file))
                os.remove(local_file)

    def restart_roles(self, *args: HadoopRoleInstance):
        raise NotImplementedError()

    def restart_cluster(self, cluster: str):
        pass

    def get_config(self, *args: 'HadoopRoleInstance', config: HadoopConfigFile) -> Dict[str, HadoopConfig]:
        configs = {}
        config_name, config_ext = config.value.split(".")

        for role in args:
            config_data = HadoopConfig(config)
            local_file = "{config}-{container}-{time}.{ext}".format(
                container=role.host, config=config_name, time=int(time.time()), ext=config_ext)
            config_file_path = self.CONFIG_FILE_PATH.format(config.value)
            role.host.download(config_file_path, local_file).run()

            config_data.xml = local_file
            config_data.merge()
            config_data.commit()

            os.remove(local_file)
            configs[role.name] = config_data

        return configs

    def replace_module_jars(self, *args: 'HadoopRoleInstance', modules: HadoopDir):
        unique_args = {role.host.get_address(): role for role in args}
        cached_found_jar = {}
        for role in unique_args.values():
            logger.info("Replacing jars on {}".format(role.host.get_address()))
            for module, jar in modules.get_jar_paths().items():
                logger.info("Replacing jar {}".format(jar))
                local_jar = jar
                if module not in cached_found_jar:
                    remote_jar_dir = self.JAR_DIR
                    if "yarn" in module:
                        remote_jar_dir += "yarn"
                    elif "mapreduce" in module:
                        remote_jar_dir += "mapreduce"

                    find_remote_jar = role.host.find_file(remote_jar_dir, "*{}*".format(module)).run()
                    remote_jar = ""
                    if find_remote_jar[0]:
                        remote_jar = find_remote_jar[0][0]
                        cached_found_jar[module] = remote_jar
                else:
                    remote_jar = cached_found_jar[module]

                if remote_jar:
                    role.host.make_backup(remote_jar).run()
                    role.host.upload(local_jar, remote_jar).run()

