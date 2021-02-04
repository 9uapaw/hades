import sys
from enum import Enum
this_module = sys.modules[__name__]


class ApplicationCommand:
    def __init__(self, path: str = None, queue: str = None):
        self.path = path
        self.queue = queue

    def build(self):
        raise NotImplementedError()


class DistributedShellApp(ApplicationCommand):

    YARN_CMD = "yarn {klass} -jar {jar} {cmd}"
    KLASS = "org.apache.hadoop.yarn.applications.distributedshell.Client"
    JAR = "{path}/yarn/hadoop-yarn-applications-distributedshell*.jar"

    def __init__(self, path: str = None, cmd: str = None, queue: str = None):
        super().__init__(path, queue)
        self.cmd = cmd or 'sleep 100'

    def build(self) -> str:
        cmd = self.YARN_CMD.format(klass=self.KLASS,
                                    jar=self.JAR.format(path=self.path),
                                    cmd="-shell_command \"{}\"".format(self.cmd))
        if self.queue:
            cmd += " -queue {}".format(self.queue)

        return cmd


class MapReduceApp(ApplicationCommand):
    MAPREDUCE_JAR = "{path}/mapreduce/hadoop-mapreduce-client-jobclient-*-tests.jar"
    YARN_CMD = "yarn jar {jar} {cmd} {prop} -m 1 -r 1 -mt 1 -rt 1"

    def __init__(self, path: str = None, cmd: str = None, queue: str = None):
        super().__init__(path, queue)
        self.cmd = cmd or 'sleep'

    def build(self):
        prop = ""
        if self.queue:
            prop += " -Dmapreduce.job.queuename={}".format(self.queue)
        cmd = self.YARN_CMD.format(jar=self.MAPREDUCE_JAR.format(path=self.path), cmd=self.cmd, prop=prop)

        return cmd


class Application(Enum):
    DISTRIBUTED_SHELL = DistributedShellApp.__name__
    MAPREDUCE = MapReduceApp.__name__
