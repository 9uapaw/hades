import dataclasses
from dataclasses import dataclass

from core.config import Config, ClusterConfig


@dataclass
class HadesContext:
    config: Config = dataclasses.field(default_factory=Config)
    cluster_config: ClusterConfig = dataclasses.field(default_factory=ClusterConfig)
    config_path: str = ""
    cluster_config_path: str = ""
