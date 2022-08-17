import datetime
import errno
import os
import shutil
import tarfile
import textwrap
import zipfile
from logging.handlers import TimedRotatingFileHandler
from typing import Callable, List

import pyfiglet

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
    return lambda line: logger.info("%s %s", target.get_colorized_output(), line.replace("\n", "")) \
        if not grep or grep(line) else ""


class FileUtils:
    @staticmethod
    def compress_files(filename: str, files: List[str]):
        file_list = " ".join(files)
        cmd = RunnableCommand(f"tar -cvf {filename} {file_list}")
        cmd.run()
        for file in files:
            LOG.debug("Removing file: %s", file)
            os.remove(file)

    @staticmethod
    def compress_dir(filename: str, dir: str):
        files = [os.path.join(dp, f) for dp, dn, filenames in os.walk(dir) for f in filenames]
        LOG.debug("Compressing dir: %s", dir)
        LOG.debug("Files: %s", files)
        cmd = RunnableCommand(f"tar -cvf {filename} -C {dir} .")
        cmd.run()
        shutil.rmtree(dir, ignore_errors=True)

    @staticmethod
    def find_files(pattern: str, dir: str = '.'):
        if pattern == "*" or pattern == ".*":
            find_cmd = f"find {dir} -print"
        else:
            find_cmd = f"find {dir} -name \"*{pattern}*\" -print"
        cmd = RunnableCommand(find_cmd)
        cmd.run()
        if not cmd.stdout:
            LOG.warning("No files found for pattern '%s' in dir '%s'", pattern, dir)
            LOG.warning(cmd.stderr)
            return ""
        return cmd.stdout

    @staticmethod
    def rm_dir(d):
        shutil.rmtree(d, ignore_errors=True)

    @staticmethod
    def copy_dir_to_temp_dir(child_dir: str, src_dir: str) -> str:
        if not os.path.isdir(src_dir):
            raise ValueError(f"Expected a source dir, got: {src_dir}")
        dest_dir = os.path.join("/tmp", child_dir)
        if os.path.exists(dest_dir):
            FileUtils.rm_dir(dest_dir)
        os.mkdir(dest_dir)

        LOG.debug("Copying %s -> %s", src_dir, dest_dir)
        shutil.copytree(src_dir, dest_dir)

        return dest_dir

    @classmethod
    def ensure_dir_created(cls, dirname, log_exception=False):
        """
        Ensure that a named directory exists; if it does not, attempt to create it.
        """
        try:
            os.makedirs(dirname)
        except OSError as e:
            if log_exception:
                LOG.exception("Failed to create dirs", exc_info=True)
            # If Errno is File exists, don't raise Exception
            if e.errno != errno.EEXIST:
                raise
        return dirname

    @classmethod
    def ensure_file_exists(cls, path, create=False):
        if not path:
            raise ValueError("Path parameter should not be None or empty!")

        if not create and not os.path.exists(path):
            raise ValueError(f"No such file or directory: {path}")

        path_comps = path.split(os.sep)
        dirs = path_comps[:-1]
        dirpath = os.sep.join(dirs)
        if not os.path.exists(dirpath):
            LOG.info("Creating dirs: %s", dirpath)
            FileUtils.ensure_dir_created(dirpath, log_exception=False)

        if not os.path.exists(path):
            # Create empty file: https://stackoverflow.com/a/12654798/1106893
            LOG.info("Creating file: %s", path)
            open(path, "a").close()


class CompressedFileUtils:
    @staticmethod
    def extract_zip_file(file: str, path: str):
        # Apparently, ZipFile does not resolve symlinks so let's do it manually
        if os.path.islink(file):
            file = os.path.realpath(file)
        FileUtils.ensure_file_exists(file)
        zip_file = zipfile.ZipFile(file, "r")
        zip_file.extractall(path)

    @staticmethod
    def extract_targz_file(file_path: str, dest_path: str):
        FileUtils.ensure_file_exists(file_path)
        file = tarfile.open(file_path)
        file.extractall(dest_path)
        file.close()

    @staticmethod
    def list_targz_file(file_path: str):
        FileUtils.ensure_file_exists(file_path)
        file = tarfile.open(file_path)
        members = file.getmembers()
        file.close()
        return [m.name for m in members]



class LoggingUtils:
    @staticmethod
    def create_file_handler(log_file_dir, level: int, fname: str = "hades"):
        file_name = f"{fname}.log"
        log_file_path = os.path.join(log_file_dir, file_name)
        fh = TimedRotatingFileHandler(log_file_path, when="midnight")
        fh.suffix = "%Y_%m_%d.log"
        fh.setLevel(level)
        return fh


class PrintUtils:
    HORIZONTAL_LINE = "============================================================\n"

    @staticmethod
    def print_banner(string):
        LOG.info(PrintUtils.HORIZONTAL_LINE)
        LOG.info(string)
        LOG.info(PrintUtils.HORIZONTAL_LINE)

    @staticmethod
    def print_banner_figlet(string):
        LOG.info(PrintUtils.HORIZONTAL_LINE)
        LOG.info(pyfiglet.figlet_format(string))
        LOG.info(PrintUtils.HORIZONTAL_LINE)


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
