import logging
import os
import time
from typing import List, Type, Dict

import yaml

from core.cmd import RunnableCommand
from core.config import ClusterConfig, ClusterRoleConfig, ClusterContextConfig
from hadoop.app.example import ApplicationCommand, MapReduceApp, DistributedShellApp
from hadoop.cluster import HadoopLogLevel
from hadoop.cluster_type import ClusterType
from hadoop.config import HadoopConfigBase
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.hadock.docker_host import DockerContainerInstance
from hadoop.hadoop_config import HadoopConfigFile
from hadoop.host import HadoopHostInstance
from hadoop.role import HadoopRoleType, HadoopRoleInstance
from hadoop_dir.module import HadoopDir

logger = logging.getLogger(__name__)


class HadockExecutor(HadoopOperationExecutor):
    DEFAULT_COMPOSE = "docker-compose.yml"
    STATUS_COLUMN = 4
    NAME_COLUMN = -1

    def __init__(self, hadock_repository: str, hadock_compose: str = None):
        self._hadock_repository = hadock_repository
        self._hadock_compose = hadock_compose or self.DEFAULT_COMPOSE

    @property
    def role_host_type(self) -> Type[HadoopHostInstance]:
        return DockerContainerInstance

    def discover(self) -> ClusterConfig:
        cluster = ClusterConfig(ClusterType.HADOCK)
        yarn_roles = {}
        hdfs_roles = {}

        with open(f"{self._hadock_repository}/{self._hadock_compose}", 'r') as f:
            compose = yaml.safe_load(f)
            for role_name, role in compose["services"].items(): # type: str, dict
                role_type = ''.join([i for i in role_name if not i.isdigit()])
                role_conf = ClusterRoleConfig()
                role_conf.type = HadoopRoleType(role_type)
                role_conf.host = role.get('container_name', role_name)
                if role_type in self.HDFS_SERVICES:
                    hdfs_roles[role_name] = role_conf
                else:
                    yarn_roles[role_name] = role_conf

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
            cmd = "docker logs {} {} {}".format(role.host,
                                                "-f" if follow else "",
                                                f"--tail {tail}" if tail else "")
            cmds.append(RunnableCommand(cmd, target=role))

        return cmds

    def set_log_level(self, *args: 'HadoopRoleInstance', package: str, level: HadoopLogLevel) -> List[RunnableCommand]:
        raise NotImplementedError()

    def get_cluster_status(self, cluster_name: str = None) -> List[HadoopClusterStatusEntry]:
        cmd = RunnableCommand("docker container list | grep bde2020/hadoop")
        stdout, stderr = cmd.run()

        status = []
        for line in stdout:
            col = list(filter(bool, line.split("   ")))
            status.append(HadoopClusterStatusEntry(col[self.NAME_COLUMN], col[self.STATUS_COLUMN]))

        return status

    def run_app(self, random_selected: HadoopRoleInstance, application: ApplicationCommand):
        logger.info("Running app %s", application.__class__.__name__)

        application.path = "/opt/hadoop/share/hadoop/"
        if isinstance(application, MapReduceApp):
            application.path += "mapreduce"
        elif isinstance(application, DistributedShellApp):
            application.path += "yarn"

        cmd = RunnableCommand(f"docker exec {random_selected.host} bash -c '{application.build()}'")

        return cmd

    def update_config(self, *args: HadoopRoleInstance, config: HadoopConfigBase, no_backup: bool = False, workdir: str = "."):
        config_name, config_ext = config.file.split(".")

        for role in args:
            logger.info("Setting config %s on %s", config.file, role.get_colorized_output())
            local_file = f"{config_name}-{role.host}-{int(time.time())}.{config_ext}"
            parent_dir = os.getcwd() if workdir == "." else workdir
            local_file_path = os.path.join(parent_dir, local_file)
            config_file_path = f"/etc/hadoop/{config.file}"
            role.host.download(config_file_path, local_file_path).run()

            config.set_base_config(local_file)
            config.merge()
            config.commit()

            role.host.upload(config.file, config_file_path).run()

            if no_backup:
                logger.info("Backup is turned off. Deleting file %s", local_file_path)
                os.remove(local_file_path)

    def restart_roles(self, *args: HadoopRoleInstance):
        cmds = []

        for role in args:
            logger.info(f"Restarting {role.get_colorized_output()}")
            restart_cmd = RunnableCommand(f"docker restart {role.host}")
            cmds.append(restart_cmd)

        return cmds

    def restart_cluster(self, cluster: str):
        pass

    def get_config(self, *args: 'HadoopRoleInstance', config: HadoopConfigFile) -> Dict[str, HadoopConfigBase]:
        configs = {}
        config_name, config_ext = config.val.split(".")

        for role in args:
            config_data = HadoopConfigBase.create(config)
            local_file = f"{config_name}-{role.host}-{int(time.time())}.{config_ext}"
            config_file_path = f"/etc/hadoop/{config.val}"
            role.host.download(config_file_path, local_file).run()

            config_data.set_base_config(local_file)
            config_data.merge()
            config_data.commit()

            os.remove(local_file)
            configs[role.name] = config_data

        return configs

    def replace_module_jars(self, *args: 'HadoopRoleInstance', modules: HadoopDir):
        logger.info("Hadock has a local mounted volume for jars. No need to replace them manually.")
