import logging
from typing import List, Tuple, Callable, Optional

import sh

from core.error import CommandExecutionException

logger = logging.getLogger(__name__)


class RunnableCommand:

    def __init__(self, cmd: str, work_dir='.', target: Optional['HadoopRoleInstance'] = None):
        self.cmd = cmd
        self.stdout: List[str] = []
        self.stderr: List[str] = []
        self.work_dir = work_dir
        self.target = target

    def run(self) -> Tuple[List[str], List[str]]:
        logger.debug("Running command {}".format(self.cmd))
        try:
            output = self.get_sync_cmd(self.cmd, self.work_dir)
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

            process = self.get_async_cmd(self.cmd, self.work_dir, stdout_callback, stderr_callback)
            if block:
                process.wait()

            return process
        except sh.ErrorReturnCode as e:
            raise CommandExecutionException("Error while executing {}".format(self.cmd), cmd=e.stderr.decode())

    def get_sync_cmd(self, c: str, cwd: str) -> any:
        return sh.bash(_cwd=cwd, c=c)

    def get_async_cmd(self, c: str, cwd: str, out: Callable[[str], None], err: Callable[[str], None]) -> any:
        sh.bash(_cwd=cwd, c=c, _bg=True, _out=out, _err=err)

    def _stdout_callback(self, res: str):
        self.stdout.append(res.replace('\n', ''))
        logger.info(res.replace("\n", ""))

    def _stderr_callback(self, res: str):
        self.stderr.append(res.replace('\n', ''))
        logger.info(res.replace("\n", ""))


class RemoteRunnableCommand(RunnableCommand):

    def __init__(self, cmd: str, user: str, host: str, target: Optional['HadoopRoleInstance'] = None):
        super().__init__(cmd, "", target)
        self.user = user
        self.host = host

    def get_sync_cmd(self, c: str, cwd: str) -> any:
        ssh = sh.ssh.bake("{user}@{host}".format(user=self.user, host=self.host))
        return ssh("bash -c \'{}\'".format(self.cmd))

    def get_async_cmd(self, c: str, cwd: str, out: Callable[[str], None], err: Callable[[str], None]) -> any:
        ssh = sh.ssh.bake("{user}@{host}".format(user=self.user, host=self.host))
        return ssh("bash -c \'{}\'".format(self.cmd), _bg=True, _out=out, _err=err)
