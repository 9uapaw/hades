import enum


class HadoopConfigFile(enum.Enum):
    YARN_SITE = "yarn-site.xml"
    CAPACITY_SCHEDULER = "capacity-scheduler.xml"
    CORE_SITE = "core-site.xml"
