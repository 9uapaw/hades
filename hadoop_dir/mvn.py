import logging

from core.cmd import RunnableCommand
from core.error import CommandExecutionException
from hadoop_dir.module import HadoopModules
import sh


logger = logging.getLogger(__name__)


class MavenCompiler:
    BASE_COMPILE_CMD = "mvn package -Pdist -Dtar -Dmaven.javadoc.skip=true -DskipTests -fail-at-end"
    MODULE_PREFIX = "org.apache.hadoop"

    def __init__(self):
        pass

    def compile(self, modules: HadoopModules):
        cmd = self.BASE_COMPILE_CMD
        for module in modules.get_modules():
            cmd += " -pl {}:{}".format(self.MODULE_PREFIX, module)

        compile_cmd = RunnableCommand(cmd, work_dir=modules.get_hadoop_dir())
        try:
            compile_cmd.run_async()
        except CommandExecutionException as e:
            err = ""
            for s in compile_cmd.stdout:
                if "ERROR" in s:
                    err += s + "\n"

            for s in compile_cmd.stderr:
                if "ERROR" in s:
                    err += s + "\n"

            if err:
                raise CommandExecutionException("Error while running compilation command\n{}".format(err), cmd)


