import enum
from abc import ABC, abstractmethod
from typing import Dict

from hadoop.executor import HadoopOperationExecutor


class HadoopServiceType(enum.Enum):
    YARN = "yarn"
    HDFS = "hdfs"


class HadoopService(ABC):

    def __init__(self, executor: HadoopOperationExecutor, name: str, roles: Dict[str, 'HadoopRoleInstance'],
                 cluster: 'HadoopCluster'):
        self._executor = executor
        self._name = name
        self._roles = roles
        self.cluster = cluster

    @property
    @abstractmethod
    def service_type(self) -> HadoopServiceType:
        raise NotImplementedError()

    def get_roles(self) -> Dict[str, 'HadoopRoleInstance']:
        return self._roles

    def add_role(self, role: 'HadoopRoleInstance'):
        role.service = self
        self._roles[role.name] = role


class YarnService(HadoopService):

    @property
    def service_type(self) -> HadoopServiceType:
        return HadoopServiceType.YARN


class HdfsService(HadoopService):

    @property
    def service_type(self) -> HadoopServiceType:
        return HadoopServiceType.HDFS
