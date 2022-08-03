import sys
from enum import Enum
this_module = sys.modules[__name__]


class ApplicationCommand:
    def __init__(self, path: str = None, queue: str = None):
        self.path = path
        self.queue = queue

    def build(self):
        raise NotImplementedError()

    def get_timeout_seconds(self):
        raise NotImplementedError()


class DistributedShellApp(ApplicationCommand):

    YARN_CMD = "yarn {klass} -jar {jar} {cmd}"
    KLASS = "org.apache.hadoop.yarn.applications.distributedshell.Client"
    JAR = "{path}/*hadoop-yarn-applications-distributedshell*.jar"

    def __init__(self, path: str = None, cmd: str = None, queue: str = None):
        super().__init__(path, queue)
        self.cmd = cmd or '-shell_command \"sleep 100\"'

    def build(self) -> str:
        cmd = self.YARN_CMD.format(klass=self.KLASS,
                                    jar=self.JAR.format(path=self.path),
                                    cmd=self.cmd)
        if self.queue:
            cmd += " -queue {}".format(self.queue)

        return cmd

    def get_timeout_seconds(self):
        raise NotImplementedError()


class MapReduceAppType(Enum):
    SLEEP = "sleep"
    PI = "pi"
    LOADGEN = "loadgen"
    RANDOM_WRITER = "randomwriter"
    TEST_MAPRED_SORT = "testmapredsort"


class MapReduceApp(ApplicationCommand):
    MAPREDUCE_JAR = "{path}/*hadoop-mapreduce-client-jobclient-*-tests.jar"
    YARN_CMD = "yarn jar {jar} {cmd}"

    def __init__(self, mr_app_type: MapReduceAppType, path: str = None, cmd: str = None, queue: str = None, timeout: int = 99999999):
        super().__init__(path, queue)
        self.name = mr_app_type.value
        self.cmd = cmd or 'sleep -m 1 -r 1 -mt 1 -rt 1'
        self.timeout = timeout

    def build(self):
        prop = ""
        if self.queue:
            prop += " -Dmapreduce.job.queuename={}".format(self.queue)
        cmd = self.YARN_CMD.format(jar=self.MAPREDUCE_JAR.format(path=self.path), cmd=self.cmd)

        return cmd

    def get_timeout_seconds(self):
        return self.timeout

    def __str__(self):
        return "{}: name: {}, command: {}".format(self.__class__.__name__, self.name, self.cmd)

    def __repr__(self):
        return str(self)


class Application(Enum):
    DISTRIBUTED_SHELL = DistributedShellApp.__name__
    MAPREDUCE = MapReduceApp.__name__
