import enum
import random

from hadoop.host import HadoopHostInstance
from colr import color

import hadoop.service


class HadoopRoleType(enum.Enum):
    RM = 'resourcemanager'
    NM = 'nodemanager'
    NN = 'namenode'
    AHS = 'app-historyserver'
    JHS = 'job-historyserver'
    DN = 'datanode'


class HadoopRoleInstance:

    def __init__(self,
                 host: HadoopHostInstance,
                 name: str,
                 role_type: HadoopRoleType,
                 service: hadoop.service.HadoopService):
        self.host = host
        self.name = name
        self.role_type = role_type
        self.service = service
        self._color = random.randint(0, 255)

    def get_colorized_output(self) -> str:
        return "[{} | {}]".format(color(self.name, fore=self._color), color(self.host, fore=self._color))

    def __str__(self):
        return "{}: Host: {}:{}".format(
            self.__class__.__name__, self.host, self.role_type.name
        )

    def __repr__(self):
        return str(self)
