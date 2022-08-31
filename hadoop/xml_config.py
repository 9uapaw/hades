import enum


class HadoopConfigFile(enum.Enum):
    YARN_SITE = "yarn-site.xml"
    CAPACITY_SCHEDULER = "capacity-scheduler.xml"
    CORE_SITE = "core-site.xml"
    MAPRED_SITE = "mapred-site.xml"
    SSL_SERVER = "ssl-server.xml"
    LOG4J_PROPERTIES = "log4j.properties"
