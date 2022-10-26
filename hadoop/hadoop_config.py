import enum


class HadoopConfigFileType(enum.Enum):
    XML = "xml"
    PROPERTIES = "properties"


class HadoopConfigFile(enum.Enum):
    YARN_SITE = ("yarn-site.xml", HadoopConfigFileType.XML)
    CAPACITY_SCHEDULER = ("capacity-scheduler.xml", HadoopConfigFileType.XML)
    CORE_SITE = ("core-site.xml", HadoopConfigFileType.XML)
    MAPRED_SITE = ("mapred-site.xml", HadoopConfigFileType.XML)
    SSL_SERVER = ("ssl-server.xml", HadoopConfigFileType.XML)
    SSL_CLIENT = ("ssl-client.xml", HadoopConfigFileType.XML)
    LOG4J_PROPERTIES = ("log4j.properties", HadoopConfigFileType.PROPERTIES)

    def __init__(self, value, type):
        self.val = value
        self.config_type = type
