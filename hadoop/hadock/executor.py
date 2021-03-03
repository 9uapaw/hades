import logging
import time
import os
from typing import List, Type, Dict

import yaml

from core.cmd import RunnableCommand
from core.config import ClusterConfig, ClusterRoleConfig, ClusterContextConfig
from hadoop.app.example import ApplicationCommand
from hadoop.cluster_type import ClusterType
from hadoop.config import HadoopConfig
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.host import HadoopHostInstance
from hadoop.hadock.docker_host import DockerContainerInstance
from hadoop.role import HadoopRoleType, HadoopRoleInstance
from hadoop.xml_config import HadoopConfigFile
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

        with open("{}/{}".format(self._hadock_repository, self._hadock_compose), 'r') as f:
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

    def read_log(self, *args: HadoopRoleInstance, follow: bool = False, tail: int or None = 10) -> List[RunnableCommand]:
        cmds = []
        for role in args:
            cmd = "docker logs {} {} {}".format(role.name,
                                                "-f" if follow else "",
                                                "--tail {}".format(tail) if tail else "")
            cmds.append(RunnableCommand(cmd, target=role))

        return cmds

    def get_cluster_status(self, cluster_name: str = None) -> List[HadoopClusterStatusEntry]:
        cmd = RunnableCommand("docker container list | grep bde2020/hadoop")
        stdout, stderr = cmd.run()

        status = []
        for line in stdout:
            col = list(filter(bool, line.split("   ")))
            status.append(HadoopClusterStatusEntry(col[self.NAME_COLUMN], col[self.STATUS_COLUMN]))

        return status

    def run_app(self, random_selected: HadoopRoleInstance, application: ApplicationCommand):
        logger.info("Running app {}".format(application.__class__.__name__))

        application.path = "/opt/hadoop/share/hadoop/yarn/"
        cmd = RunnableCommand("docker exec {} bash -c '{}'".format(random_selected.host, application.build()))
        cmd.run_async()

    def update_config(self, *args: HadoopRoleInstance, config: HadoopConfig, no_backup: bool = False):
        config_name, config_ext = config.file.split(".")

        for role in args:
            logger.info("Setting config {} on {}".format(config.file, role.get_colorized_output()))
            local_file = "{config}-{container}-{time}.{ext}".format(
                container=role.host, config=config_name, time=int(time.time()), ext=config_ext)
            config_file_path = "/etc/hadoop/{}".format(config.file)
            role.host.download(config_file_path, local_file).run()

            config.xml = local_file
            config.merge()
            config.commit()

            role.host.upload(config.file, config_file_path).run()
            os.remove(config.file)

            if no_backup:
                logger.info("Backup is turned off. Deleting file {}".format(local_file))
                os.remove(local_file)

    def restart_roles(self, *args: HadoopRoleInstance):
        for role in args:
            logger.info("Restarting {}".format(role.get_colorized_output()))
            restart_cmd = RunnableCommand("docker restart {}".format(role.host))
            logger.info(restart_cmd.run())

    def get_config(self, *args: 'HadoopRoleInstance', config: HadoopConfigFile) -> Dict[str, HadoopConfig]:
        pass

    def replace_module_jars(self, *args: 'HadoopRoleInstance', modules: HadoopDir):
        logger.info("Hadock has a local mounted volume for jars. No need to replace them manually.")
