from abc import ABC


class HadoopHostInstance(ABC):

    def __init__(self, address: str):
        self._address = address

    def __repr__(self) -> str:
        return self._address

    def __str__(self):
        return self._address

    def get_address(self) -> str:
        return self._address


class RemoteHostInstance(HadoopHostInstance):
    pass


class DockerContainerInstance(HadoopHostInstance):

    def get_address(self) -> str:
        return "localhost"
