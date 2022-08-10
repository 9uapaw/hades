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
        logger.info("Copying file from %s:%s to local:%s", self, source, dest)
        return RunnableCommand(f"docker cp {self.address}:{source} {dest}")

    def upload(self, source: str, dest: str) -> RunnableCommand:
        logger.info("Copying file from local:%s to %s:%s", source, self, dest)
        return RunnableCommand(f"docker cp {source} {self.address}:{dest}")

    def get_address(self) -> str:
        return "localhost"

    def create_cmd(self, cmd: str) -> RunnableCommand:
        return RunnableCommand(f"docker exec {self.get_address()} bash -c '{cmd}'", target=self.role)
