import yaml

from format.cli_formatter import CliFormat


class BlobFormat(CliFormat):

    def __init__(self, data: dict):
        self._internal = data

    def format(self) -> str:
        return yaml.dump(self._internal)
