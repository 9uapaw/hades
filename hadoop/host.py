from abc import ABC


class HadoopHostInstance(ABC):

    def __init__(self, address: str, user: str):
        self.address = address
        self.user = user

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


class RemoteHostInstance(HadoopHostInstance):
    pass
