from pythoncommons.file_utils import FileUtils, FindResultType
from pythoncommons.project_utils import SimpleProjectUtils
import logging

from core.error import HadesException

REPO_ROOT_DIRNAME = "hades"
LOG = logging.getLogger(__name__)


class LocalDirs:
    REPO_ROOT_DIR = FileUtils.find_repo_root_dir(__file__, REPO_ROOT_DIRNAME)


class LocalFiles:

    @staticmethod
    def get_unique_file(file_name: str):
        found_files = FileUtils.find_files(
            LocalDirs.REPO_ROOT_DIR,
            find_type=FindResultType.FILES,
            single_level=False,
            full_path_result=True,
            regex=file_name
        )
        if not found_files:
            raise HadesException("Could not find file with name: {}. Repo root dir was: {}"
                                 .format(file_name, LocalDirs.REPO_ROOT_DIR))
        if len(found_files) > 1:
            raise HadesException("Multiple find results for name: {}. Results: {}, Repo root dir was: {}"
                                 .format(file_name, found_files, LocalDirs.REPO_ROOT_DIR))
        return found_files[0]
