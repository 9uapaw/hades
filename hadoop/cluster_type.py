import enum


class ClusterType(enum.Enum):
    CM = "ClouderaManager"
    HADOCK = "Hadock"
    STANDARD = "Standard"