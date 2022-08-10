import logging

from core.cmd import RunnableCommand
from core.config import Config
from core.error import CommandExecutionException
from hadoop_dir.module import HadoopDir, HadoopModule

logger = logging.getLogger(__name__)


class MavenCompiler:
    MODULE_PREFIX = "org.apache.hadoop"

    def __init__(self, config: Config):
        self._config = config

    def compile(self, modules: HadoopDir):
        cmd = self._config.compile_cmd
        for module in modules.get_modules():
            cmd += f" -pl {self.MODULE_PREFIX}:{module}"

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
                raise CommandExecutionException(f"Error while running compilation command\n{err}", cmd)

    def compile_single_module(self, hadoop_dir: HadoopDir, module: HadoopModule):
        compile_cmd = RunnableCommand(self._config.compile_cmd, work_dir=hadoop_dir.get_module_abs_path(module))
        compile_cmd.run_async()
