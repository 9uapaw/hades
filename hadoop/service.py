import enum
from abc import ABC, abstractmethod
from typing import Dict

from hadoop.executor import HadoopOperationExecutor
from hadoop.role import HadoopRoleInstance


class HadoopServiceType(enum.Enum):
    YARN = "yarn"
    HDFS = "hdfs"


class HadoopService(ABC):

    def __init__(self, executor: HadoopOperationExecutor, name: str, roles: Dict[str, HadoopRoleInstance],
                 cluster_name: str = ''):
        self._executor = executor
        self._name = name
        self._roles = roles
        self._cluster_name = cluster_name

    @property
    @abstractmethod
    def service_type(self) -> HadoopServiceType:
        raise NotImplementedError()

    def get_roles(self) -> Dict[str, HadoopRoleInstance]:
        return self._roles


class YarnService(HadoopService):

    @property
    def service_type(self) -> HadoopServiceType:
        return HadoopServiceType.YARN


class HdfsService(HadoopService):

    @property
    def service_type(self) -> HadoopServiceType:
        return HadoopServiceType.HDFS
