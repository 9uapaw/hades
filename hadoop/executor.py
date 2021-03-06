from abc import ABC, abstractmethod
from typing import List, Type, Dict

from core.cmd import RunnableCommand
from core.config import ClusterConfig
from hadoop.app.example import ApplicationCommand
from hadoop.config import HadoopConfig
from hadoop.data.status import HadoopClusterStatusEntry, HadoopConfigEntry
from hadoop.host import HadoopHostInstance
from hadoop.xml_config import HadoopConfigFile
from hadoop_dir.module import HadoopModule, HadoopDir


class HadoopOperationExecutor(ABC):
    HDFS_SERVICES = ["namenode", "datanode"]

    @property
    @abstractmethod
    def role_host_type(self) -> Type[HadoopHostInstance]:
        raise NotImplementedError()

    @abstractmethod
    def discover(self) -> ClusterConfig:
        raise NotImplementedError()

    @abstractmethod
    def read_log(self, *args: 'HadoopRoleInstance', follow: bool = False, tail: int or None = 10, download: bool = None) -> List[RunnableCommand]:
        raise NotImplementedError()

    @abstractmethod
    def get_cluster_status(self, cluster_name: str = None) -> List[HadoopClusterStatusEntry]:
        raise NotImplementedError()

    @abstractmethod
    def run_app(self, random_selected: 'HadoopRoleInstance', application: ApplicationCommand) -> RunnableCommand:
        raise NotImplementedError()

    @abstractmethod
    def update_config(self, *args: 'HadoopRoleInstance', config: HadoopConfig, no_backup: bool):
        raise NotImplementedError()

    @abstractmethod
    def restart_roles(self, *args: 'HadoopRoleInstance'):
        raise NotImplementedError()

    @abstractmethod
    def restart_cluster(self, cluster: str):
        raise NotImplementedError()

    def get_config(self, *args: 'HadoopRoleInstance', config: HadoopConfigFile) -> Dict[str, HadoopConfig]:
        raise NotImplementedError()

    def replace_module_jars(self, *args: 'HadoopRoleInstance', modules: HadoopDir):
        raise NotImplementedError()

