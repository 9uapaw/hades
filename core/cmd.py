import dataclasses
import logging
from dataclasses import dataclass
from typing import List, Tuple, Callable, Optional

import sh

from core.error import CommandExecutionException, HadesCommandTimedOutException

logger = logging.getLogger(__name__)


@dataclass
class RunnableCommand:
    cmd: str
    work_dir: str = '.'
    target: Optional['HadoopRoleInstance'] = None
    stdout: List[str] = dataclasses.field(default_factory=list)
    stderr: List[str] = dataclasses.field(default_factory=list)
    _cmd_prefix = ""

    def __key(self):
        return self.cmd, self.work_dir, self._cmd_prefix, self.target

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, RunnableCommand):
            return self.__key() == other.__key()
        return NotImplemented

    def run(self) -> Tuple[List[str], List[str]]:
        logger.debug("Running command %s", self.cmd)
        try:
            output = self.get_sync_cmd(self.cmd, self.work_dir)
            self.stdout.extend(list(filter(bool, output.stdout.decode().split("\n"))))
            self.stderr.extend(list(filter(bool, output.stderr.decode().split("\n"))))
            return self.stdout, self.stderr
        except sh.ErrorReturnCode as e:
            raise CommandExecutionException(str(e), self.cmd, self._convert_output(e.stderr.decode()),
                                            self._convert_output(e.stdout.decode()))

    def run_async(self, stdout: Callable[[str], None] = None, stderr: Callable[[str], None] = None, block=False, timeout=-1):
        try:
            logger.debug("Running command asynchronously %s as blocking %s", self.cmd, block)
            stdout_callback = self._stdout_callback
            stderr_callback = stdout_callback
            if stdout:
                stdout_callback = stdout
            if stderr:
                stderr_callback = stderr

            # TODO timeout should be used for async_cmd as well (??) --> https://stackoverflow.com/a/25616495/1106893
            process = self.get_async_cmd(self.cmd, self.work_dir, stdout_callback, stderr_callback)
            if block:
                process.wait(timeout=timeout)

            return process

        except sh.TimeoutException as e:
            raise HadesCommandTimedOutException(f"Error while executing {self.cmd}",
                                                cmd=self.cmd)
        except sh.ErrorReturnCode as e:
            raise CommandExecutionException(f"Error while executing {self.cmd}",
                                            cmd=self.cmd,
                                            stdout=self.extract_stdout(e),
                                            stderr=self.extract_stderr(e))

    def get_sync_cmd(self, c: str, cwd: str) -> any:
        return sh.bash(_cwd=cwd, c=c)

    def get_async_cmd(self, c: str, cwd: str, out: Callable[[str], None], err: Callable[[str], None]) -> any:
        return sh.bash(_cwd=cwd, c=c, _bg=True, _out=out, _err=err)

    def _stdout_callback(self, res: str):
        # logger.debug("appending stdout...")
        self.stdout.append(res.replace('\n', ''))
        logger.info(res.replace("\n", ""))

    def _stderr_callback(self, res: str):
        # logger.debug("appending stderr...")
        self.stderr.append(res.replace('\n', ''))
        logger.info(res.replace("\n", ""))

    @staticmethod
    def _convert_output(output: str) -> List[str]:
        return list(filter(bool, output.split("\n")))

    def set_cmd_prefix(self, prefix: str):
        self._cmd_prefix = prefix

    def extract_stderr(self, e):
        err = e.stderr.decode()
        if err:
            return err
        return self.stderr

    def extract_stdout(self, e):
        out = e.stdout.decode()
        if out:
            return out
        return self.stdout


class RemoteRunnableCommand(RunnableCommand):

    def __init__(self, cmd: str, user: str, host: str, target: Optional['HadoopRoleInstance'] = None):
        super().__init__(cmd, "", target)
        self.user = user
        self.host = host

    def get_sync_cmd(self, c: str, cwd: str, login_shell=True) -> any:
        ssh = sh.ssh.bake(f"{self.user}@{self.host}")

        switches = self._prepare_switches(login_shell)
        cmd = f"bash {switches} \'{self._cmd_prefix} {self.cmd}\'"
        return ssh(cmd)

    def get_async_cmd(self, c: str, cwd: str, out: Callable[[str], None], err: Callable[[str], None], login_shell=True) -> any:
        ssh = sh.ssh.bake(f"{self.user}@{self.host}")

        switches = self._prepare_switches(login_shell)
        cmd = f"bash {switches} \'{self._cmd_prefix} {self.cmd}\'"
        return ssh(cmd, _bg=True, _out=out, _err=err)

    @staticmethod
    def _prepare_switches(login_shell):
        switches = "-l " if login_shell else ""
        switches += "-c "
        return switches


class DownloadCommand(RunnableCommand):
    def __init__(self, cmd: str, dest: str, local_file: str, work_dir='.', target: Optional['HadoopRoleInstance'] = None):
        super().__init__(cmd, work_dir=work_dir, target=target)
        self.dest = dest
        self.local_file = local_file
