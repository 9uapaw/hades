import logging
from typing import List, Tuple, Callable, Optional

import sh

from core.error import CommandExecutionException
from hadoop.role import HadoopRoleInstance

logger = logging.getLogger(__name__)


class RunnableCommand:

    def __init__(self, cmd: str, work_dir='.', target: Optional[HadoopRoleInstance] = None):
        self.cmd = cmd
        self.stdout: List[str] = []
        self.stderr: List[str] = []
        self.work_dir = work_dir
        self.target = target

    def run(self) -> Tuple[List[str], List[str]]:
        logger.debug("Running command {}".format(self.cmd))
        try:
            output = sh.bash(_cwd=self.work_dir, c=self.cmd)
            self.stdout = list(filter(bool, output.stdout.decode().split("\n")))
            self.stderr = list(filter(bool, output.stderr.decode().split("\n")))
            return self.stdout, self.stderr
        except Exception as e:
            raise CommandExecutionException(str(e), self.cmd)

    def run_async(self, stdout: Callable[[str], None] = None, stderr: Callable[[str], None] = None, block=True):
        try:
            logger.debug("Running command asynchronously {} as blocking {}".format(self.cmd, block))
            stdout_callback = self._stdout_callback
            stderr_callback = stdout_callback
            if stdout:
                stdout_callback = stdout
            if stderr:
                stderr_callback = stderr

            process = sh.bash(_cwd=self.work_dir, c=self.cmd, _bg=True, _out=stdout_callback, _err=stderr_callback)
            if block:
                process.wait()

            return process
        except sh.ErrorReturnCode as e:
            raise CommandExecutionException("Error while executing {}".format(self.cmd), cmd=e.stderr.decode())

    def _stdout_callback(self, res: str):
        self.stdout.append(res.replace('\n', ''))
        logger.info(res.replace("\n", ""))

    def _stderr_callback(self, res: str):
        self.stderr.append(res.replace('\n', ''))
        logger.info(res.replace("\n", ""))

