from abc import ABC

from core.cmd import RunnableCommand, RemoteRunnableCommand


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

    def upload(self, source: str, dest: str):
        raise NotImplementedError()

    def download(self, source: str, dest: str):
        raise NotImplementedError()

    def run_cmd(self, cmd: str) -> RunnableCommand:
        raise NotImplementedError()


class RemoteHostInstance(HadoopHostInstance):

    def upload(self, source: str, dest: str):
        cmd = RunnableCommand("scp {source} {user}@{host}:{dest}".format(source=source, user=self.user, host=self.get_address(), dest=dest))
        cmd.run()

    def download(self, source: str, dest: str):
        cmd = RunnableCommand("scp {user}@{host}:{source} {dest}".format(source=source, user=self.user, host=self.get_address(), dest=dest))
        cmd.run()

    def run_cmd(self, cmd: str) -> RunnableCommand:
        prefix = self.role.service.cluster.ctx.config.cmd_prefix
        pre_hook = self.role.service.cluster.ctx.config.cmd_hook
        if prefix:
            cmd = "{} {}".format(prefix, cmd)
        if pre_hook:
            cmd = "{} && {}".format(pre_hook, cmd)

        return RemoteRunnableCommand(cmd, self.user, self.get_address(), self.role)
