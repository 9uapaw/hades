from abc import ABC


class HadoopHostInstance(ABC):
    pass

    def get_host(self) -> str:
        raise NotImplementedError()


class CmHostInstance(HadoopHostInstance):

    def __init__(self, host: str):
        self._host = host

    def __repr__(self) -> str:
        return self._host

    def __str__(self):
        return self._host

    def get_host(self) -> str:
        return self._host


class DockerContainerInstance(HadoopHostInstance):

    def __init__(self, container: str):
        self._container = container

    def __repr__(self) -> str:
        return self._container

    def __str__(self):
        return self._container

    def get_host(self) -> str:
        return "localhost"
