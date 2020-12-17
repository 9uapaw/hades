from core.config import ClusterConfig
from hadoop.cm.cm_api import CmApi
from hadoop.executor import HadoopOperationExecutor


class CmExecutor(HadoopOperationExecutor):

    def __init__(self, cm_api: CmApi):
        self._cm_api = cm_api

    def discover(self) -> ClusterConfig:
        pass