import enum


class HadoopAuthentication(enum.Enum):
    SIMPLE = "SIMPLE"
    SECURE = "KERBEROS"


class HadoopResponseFormat(enum.Enum):
    XML = "xml"
    JSON = "json"
    TEXT = "text"
