import dataclasses
from dataclasses import dataclass
from typing import List, Dict

from dataclasses_json import dataclass_json, LetterCase


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class ClusterRoleConfig:
    type: str = ''
    host: str = ''


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class ClusterContextConfig:
    name: str = ''
    roles: Dict[str, ClusterRoleConfig] = dataclasses.field(default_factory=dict)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class ClusterConfig:
    cluster_type: str = 'STANDARD'
    cluster_name: str = ''
    context: Dict[str, ClusterContextConfig] = dataclasses.field(default_factory=dict)
    specific_context: Dict[str, str] = dataclasses.field(default_factory=dict)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class Config:
    hadoop_path: str = ''
    hadoop_jar_path: str = ''
    cluster: ClusterConfig = dataclasses.field(default_factory=lambda: ClusterConfig())
    default_modules: List[str] = dataclasses.field(default_factory=lambda: ["hadoop-common", "hadoop-yarn-server-common", "hadoop-yarn-api"])

    @classmethod
    def from_file(cls, path: str) -> 'Config':
        with open(path, 'r') as f:
            return Config.from_json(f.read())
