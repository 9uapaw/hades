from dataclasses import dataclass

from core.config import Config


@dataclass
class HadesContext:
    config: Config
