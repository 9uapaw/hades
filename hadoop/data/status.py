import enum
from dataclasses import dataclass


class HadoopClusterStatusType(enum.Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


@dataclass
class HadoopClusterStatusEntry:
    name: str
    status: str


@dataclass
class HadoopConfigEntry:
   property: str
   value: str
