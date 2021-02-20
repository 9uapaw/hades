from typing import List, Type

from core.cmd import RunnableCommand
from core.config import ClusterConfig
from hadoop.app.example import ApplicationCommand
from hadoop.cm.cm_api import CmApi
from hadoop.config import HadoopConfig
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.host import HadoopHostInstance, RemoteHostInstance
from hadoop.role import HadoopRoleInstance


class CmExecutor(HadoopOperationExecutor):

    def __init__(self, cm_api: CmApi):
        self._cm_api = cm_api

    @property
    def role_host_type(self) -> Type[HadoopHostInstance]:
        return RemoteHostInstance

    def discover(self) -> ClusterConfig:
        pass

    def read_log(self, *args: HadoopRoleInstance, follow: bool = False, tail: int or None = 10) -> List[RunnableCommand]:
        pass

    def get_cluster_status(self, cluster_name: str = None) -> List[HadoopClusterStatusEntry]:
        pass

    def run_app(self, random_selected: HadoopRoleInstance, application: ApplicationCommand):
        pass

    def update_config(self, *args: HadoopRoleInstance, config: HadoopConfig, no_backup: bool):
        pass

    def restart_roles(self, *args: HadoopRoleInstance):
        pass


