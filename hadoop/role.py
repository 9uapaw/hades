import enum
import random

from hadoop.host import HadoopHostInstance
from colr import color


class HadoopRoleType(enum.Enum):
    RM = 'resourcemanager'
    NM = 'nodemanager'
    NN = 'namenode'
    AHS = 'app-historyserver'
    JHS = 'job-historyserver'
    DN = 'datanode'


class HadoopRoleInstance:

    def __init__(self,
                 service_name: str,
                 host: HadoopHostInstance,
                 name: str,
                 role_type: HadoopRoleType,
                 cluster_name: str = ''):
        self.host = host
        self.name = name
        self.role_type = role_type
        self._service_name = service_name
        self._cluster_name = cluster_name
        self._color = random.randint(0, 255)

    def get_colorized_output(self) -> str:
        return "[{} | {}]".format(color(self.name, fore=self._color), color(self.host, fore=self._color))

