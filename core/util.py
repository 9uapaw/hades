import datetime
import os
import shutil
import textwrap
from logging.handlers import TimedRotatingFileHandler
from typing import Callable, List

from core.cmd import RunnableCommand
from hadoop.role import HadoopRoleInstance

import logging
LOG = logging.getLogger(__name__)


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


class FileUtils:
    @staticmethod
    def compress_files(filename: str, files: List[str]):
        cmd = RunnableCommand("tar -cvf {fname} {files}".format(fname=filename, files=" ".join(files)))
        cmd.run()
        for file in files:
            LOG.debug("Removing file: %s", file)
            os.remove(file)

    @staticmethod
    def compress_dir(filename: str, dir: str):
        cmd = RunnableCommand("tar -cvf {fname} -C {dir} .".format(fname=filename, dir=dir))
        cmd.run()
        shutil.rmtree(dir, ignore_errors=True)

    @staticmethod
    def find_files(pattern: str, dir: str = '.'):
        find_cmd = RunnableCommand("find {dir} -name \"*{pattern}*\" -print".format(dir=dir, pattern=pattern))
        find_cmd.run()
        if not find_cmd.stdout:
            LOG.warning("No files found for pattern '%s' in dir '%s'", pattern, dir)
            LOG.warning(find_cmd.stderr)
            return ""
        return find_cmd.stdout


class LoggingUtils:
    @staticmethod
    def create_file_handler(log_file_dir, level: int, fname: str = "hades"):
        file_name = f"{fname}.log"
        log_file_path = os.path.join(log_file_dir, file_name)
        fh = TimedRotatingFileHandler(log_file_path, when="midnight")
        fh.suffix = "%Y_%m_%d.log"
        fh.setLevel(level)
        return fh


class DateUtils:
    @staticmethod
    def get_current_datetime(fmt="%Y%m%d_%H%M%S"):
        return DateUtils.now_formatted(fmt)

    @classmethod
    def now(cls):
        return datetime.datetime.now()

    @classmethod
    def now_formatted(cls, fmt):
        return DateUtils.now().strftime(fmt)
