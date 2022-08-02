import logging

from core.cmd import RunnableCommand
from hadoop.host import HadoopHostInstance


logger = logging.getLogger(__name__)


class DockerContainerInstance(HadoopHostInstance):

    def find_file(self, dir: str, search: str) -> RunnableCommand:
        raise NotImplementedError()

    def make_backup(self, dest: str) -> RunnableCommand:
        raise NotImplementedError()

    def download(self, source: str, dest: str = None) -> RunnableCommand:
        logger.info("Copying file from {}:{} to local:{}".format(self, source, dest))
        return RunnableCommand("docker cp {container}:{source} {dest}".format(dest=dest, container=self.address, source=source))

    def upload(self, source: str, dest: str) -> RunnableCommand:
        logger.info("Copying file from local:{} to {}:{}".format(source, self, dest))
        return RunnableCommand("docker cp {source} {container}:{dest}".format(dest=dest, container=self.address, source=source))

    def get_address(self) -> str:
        return "localhost"

    def create_cmd(self, cmd: str) -> RunnableCommand:
        return RunnableCommand("docker exec {} bash -c '{}'".format(self.get_address(), cmd), target=self.role)
