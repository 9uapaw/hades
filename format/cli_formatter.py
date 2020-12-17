from abc import ABC


class CliFormat(ABC):

    def format(self) -> str:
        raise NotImplementedError()