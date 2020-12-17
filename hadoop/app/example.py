import sys
from enum import Enum
this_module = sys.modules[__name__]


class ApplicationCommand:
    def __init__(self, path: str = None):
        self.path = path

    def build(self):
        raise NotImplementedError()


class DistributedShellApp(ApplicationCommand):

    YARN_CMD = "yarn {klass} -jar {jar} {cmd}"
    KLASS = "org.apache.hadoop.yarn.applications.distributedshell.Client"
    JAR = "{path}/yarn/hadoop-yarn-applications-distributedshell*.jar"

    def __init__(self, path: str = None, cmd: str = None):
        super().__init__(path)
        self.cmd = cmd or 'sleep 100'

    def build(self) -> str:
        return self.YARN_CMD.format(klass=self.KLASS,
                                    jar=self.JAR.format(path=self.path),
                                    cmd="-shell_command \"{}\"".format(self.cmd))


class MapReduceApp(ApplicationCommand):
    MAPREDUCE_JAR = "{path}/mapreduce/hadoop-mapreduce-examples*.jar"
    YARN_CMD = "yarn jar {jar} {cmd}"

    def __init__(self, path: str = None, cmd: str = None):
        super().__init__(path)
        self.cmd = cmd or 'pi 16 100000'

    def build(self):
        return self.YARN_CMD.format(jar=self.MAPREDUCE_JAR.format(path=self.path), cmd=self.cmd)


class Application(Enum):
    DISTRIBUTED_SHELL = DistributedShellApp.__name__
    MAPREDUCE = MapReduceApp.__name__
