import logging

from core.cmd import RunnableCommand
from hadoop.host import HadoopHostInstance


logger = logging.getLogger(__name__)


class DockerContainerInstance(HadoopHostInstance):

    def download(self, source: str, dest: str):
        logger.info("Copying file from {}:{} to local:{}".format(self, source, dest))
        res = RunnableCommand("docker cp {container}:{source} {dest}".format(dest=dest, container=self.address, source=source)).run()
        if res:
            logger.info(res)

    def upload(self, source: str, dest: str):
        logger.info("Copying file from local:{} to {}:{}".format(source, self, dest))
        res = RunnableCommand("docker cp {source} {container}:{dest}".format(dest=dest, container=self.address, source=source)).run()
        if res:
            logger.info(res)

    def get_address(self) -> str:
        return "localhost"