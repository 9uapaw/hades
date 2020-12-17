from dataclasses import is_dataclass, fields
from typing import List, Dict

import tabulate

from format.cli_formatter import CliFormat


class TableFormat(CliFormat):

    def __init__(self, table: List[List[str]]):
        self._internal: List[List[str]] = table

    @classmethod
    def from_dataclass(cls, data: List[any]) -> 'TableFormat':
        internal = []
        if len(data) == 0:
            raise ValueError("data is empty")
        internal.append([field.name for field in fields(data[0])])

        for d in data:
            if not is_dataclass(d):
                raise ValueError("data is not a dataclass")

            embedded = []
            for field in fields(d):
                embedded.append(str(getattr(d, field.name)))

            internal.append(embedded)

        return TableFormat(internal)

    @classmethod
    def from_dict(cls, data: List[Dict[str, str]]) -> 'TableFormat':
        internal = []
        if len(data) == 0:
            raise ValueError("data is empty")
        internal.append(list(data[0].keys()))

        for d in data:
            internal.append(list([v for v in d.values() if type(v) == str or type(v) == int or type(v) == float]))

        return TableFormat(internal)

    def format(self) -> str:
        return tabulate.tabulate(self._internal)
