import logging
import os
import time
from abc import ABC
from pathlib import PurePath

from core.cmd import RunnableCommand, RemoteRunnableCommand, DownloadCommand

logger = logging.getLogger(__name__)


class HadoopHostInstance(ABC):

    def __init__(self, role: 'HadoopRoleInstance', address: str, user: str):
        self.address = address
        self.user = user
        self.role = role

    def __repr__(self) -> str:
        return self.address

    def __str__(self):
        return self.address

    def get_address(self) -> str:
        return self.address

    def upload(self, source: str, dest: str) -> RunnableCommand:
        raise NotImplementedError()

    def download(self, source: str, dest: str = None) -> DownloadCommand:
        raise NotImplementedError()

    def find_file(self, dir: str, search: str) -> RunnableCommand:
        raise NotImplementedError()

    def create_cmd(self, cmd: str) -> RunnableCommand:
        raise NotImplementedError()

    def make_backup(self, dest: str) -> RunnableCommand:
        raise NotImplementedError()


class RemoteHostInstance(HadoopHostInstance):
    HADES_BACKUP_DIR = "/tmp/hades-bkp"

    def upload(self, source: str, dest: str) -> RunnableCommand:
        return RunnableCommand("scp {source} {user}@{host}:{dest}".format(source=source, user=self.user, host=self.get_address(), dest=dest), target=self.role)

    def download(self, source: str, dest: str = None) -> RunnableCommand:
        if not dest:
            dest = os.getcwd()
        if os.path.isdir(dest):
            dest_file = os.path.basename(source)
        else:
            dest_file = os.path.basename(dest)
        cmd = "scp {user}@{host}:{source} {dest}".format(source=source, user=self.user,
                                                         host=self.get_address(), dest=dest)
        return DownloadCommand(cmd, target=self.role, dest=dest, local_file=dest_file)

    def make_backup(self, dest: str) -> RunnableCommand:
        dest = PurePath(dest)
        backup = "{bkp_dir}/{file}-{time}{suffix}".format(bkp_dir=self.HADES_BACKUP_DIR, file=dest.stem,
                                                          time=int(time.time()), suffix=dest.suffix)

        logger.info("Backup file {} as {}".format(dest, backup))
        return RemoteRunnableCommand("mkdir -p {} && cp {} {}".format(self.HADES_BACKUP_DIR, dest, backup), self.user, self.get_address(), target=self.role)

    def find_file(self, dir: str, search: str) -> RunnableCommand:
        return self.create_cmd("find {source} -name {search} -print".format(source=dir, search=search))

    def create_cmd(self, cmd: str) -> RunnableCommand:
        prefix = self.role.service.cluster.ctx.config.cmd_prefix
        cmds = [cmd for cmd in self.role.service.cluster.ctx.config.cmd_hook]
        if prefix:
            cmd = "{} {}".format(prefix, cmd)

        cmds.append(cmd)
        cmd = " && ".join(cmds)

        return RemoteRunnableCommand(cmd, self.user, self.get_address(), self.role)
