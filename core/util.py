import logging
import textwrap


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