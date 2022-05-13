import logging
import textwrap
from typing import Callable

from hadoop.role import HadoopRoleInstance


class Formatter(logging.Formatter):
    def __init__(self, format_str):
        super(Formatter, self).__init__(fmt=format_str)

    def format(self, record):
        message = record.msg
        record.msg = ''
        header = super(Formatter, self).format(record)
        msg = textwrap.indent(message, ' ' * len(header)).strip()
        record.msg = message
        return header + msg


def generate_role_output(logger: logging.Logger, target: HadoopRoleInstance, grep: Callable) -> Callable[[str], None]:
    return lambda line: logger.info("{} {}".format(target.get_colorized_output(), line.replace("\n", ""))) \
        if not grep or grep(line) else ""
