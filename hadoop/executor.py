from abc import ABC, abstractmethod
from typing import List

from core.cmd import RunnableCommand
from core.config import ClusterConfig
from hadoop.app.example import ApplicationCommand
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.role import HadoopRoleInstance
from hadoop.xml_config import HadoopConfigFile


class HadoopOperationExecutor(ABC):

    @abstractmethod
    def discover(self) -> ClusterConfig:
        raise NotImplementedError()

    @abstractmethod
    def read_log(self, *args: HadoopRoleInstance, follow: bool = False, tail: int or None = 10) -> List[RunnableCommand]:
        raise NotImplementedError()

    def get_cluster_status(self, cluster_name: str = None) -> List[HadoopClusterStatusEntry]:
        raise NotImplementedError()

    def run_app(self, random_selected: HadoopRoleInstance, application: ApplicationCommand):
        raise NotImplementedError()

    def update_config(self, *args: HadoopRoleInstance, file: HadoopConfigFile, properties: List[str], values: List[str], no_backup: bool = False, source: str = None):
        raise NotImplementedError()

    def restart_roles(self, *args: HadoopRoleInstance):
        raise NotImplementedError()
