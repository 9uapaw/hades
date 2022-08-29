import enum
import glob
import logging
import distutils
import re
from distutils import dir_util
from typing import Dict, List
import shutil

from core.cmd import RunnableCommand
from core.error import CommandExecutionException, HadesException

logger = logging.getLogger(__name__)


class HadoopModule(enum.Enum):
    YARN_UI2 = "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-ui"
    HADOOP_DIST = "hadoop-dist"
    RESOURCEMANAGER = "hadoop-yarn-server-resourcemanager"
    MR_CLIENT_SHUFFLE = "hadoop-mapreduce-client-shuffle"
    YARN_COMMON = "hadoop-yarn-common"


class HadoopDir:
    CHANGED_MODULES_CMD = "git status --porcelain | grep \".*hadoop.*\" | sed -E \"s/.*\\/(.*)\\/src.*/\\1/g\""
    # CHANGED_MODULES_CMD = "git status --porcelain | grep \".*hadoop.*\" | sed -E \"s/.*\\/(.*)\\/src.*/\\1/g\" | sed -E \"s/.*\\/(.*)\\/pom\\.xml/\\1/g\""
    SWITCH_BRANCH_CMD_TEMPLATE = "git checkout {}"
    GET_BRANCH_CMD = "git rev-parse --abbrev-ref HEAD"
    RESET_HARD_CMD_TEMPLATE = "git reset {} --hard"
    APPLY_PATCH_CMD_TEMPLATE = "git apply {}"
    FIND_JAR_OF_MODULE_TEMPLATE = "find . -name \"*{module}*\" -print | grep \".*{module}/target.*-SNAPSHOT.jar\""

    MAPREDUCE_JAR_DIR = "hadoop/mapreduce"
    HDFS_JAR_DIR = "hadoop/hdfs"
    YARN_JAR_DIR = "hadoop/yarn"

    YARN_UI2_MODULE_PATH = HadoopModule.YARN_UI2.value + "/target/hadoop-yarn-ui-*-SNAPSHOT"
    YARN_UI2_DIST_PATH = HadoopModule.HADOOP_DIST.value + "/target/hadoop-*-SNAPSHOT/share/hadoop/yarn/webapps/ui2"

    def __init__(self, hadoop_dir: str):
        self._modules: Dict[str, str] = {}
        self._changed: Dict[str, str] = {}
        self._hadoop_dir = hadoop_dir

    def extract_changed_modules(self, allow_empty: bool = False):
        logger.info("Searching modules in hadoop dir %s", self._hadoop_dir)
        module_cmd = RunnableCommand(self.CHANGED_MODULES_CMD, work_dir=self._hadoop_dir)

        module_cmd.run()
        stdout = module_cmd.stdout
        if not allow_empty and not stdout:
            raise CommandExecutionException("\n".join(stdout), self.CHANGED_MODULES_CMD)

        modules = set(stdout)
        for module in modules:
            m = re.search('\\s*M ', module)
            if m:
                logger.warning("Ignoring changed file, could not determine module for: %s", module)
                continue
            try:
                jar = self._find_jar(module)
                logger.debug("Found jar '%s' for module '%s'", jar, module)
                self._modules[module] = jar
                self._changed[module] = self._modules[module]
            except CommandExecutionException as e:
                logger.warning(e)
                if not e.stderr:
                    continue
                else:
                    raise e
        logger.debug("Discovered changed modules: %s", self._changed)

    def copy_modules_to_dist(self, dest: str, *args):
        if not args:
            args = list(self._modules.keys())

        for module in args:
            if module in self._modules:
                original_jar = self._modules[module]
                if not original_jar:
                    continue
                new_path = self.YARN_JAR_DIR
                if "mapreduce" in module:
                    new_path = self.MAPREDUCE_JAR_DIR
                elif "hdfs" in module:
                    new_path = self.HDFS_JAR_DIR

                full_path = f"{dest}/{new_path}"
                logger.info("Copying %s to %s", original_jar, full_path)
                shutil.copy2(original_jar, full_path)

    def get_module_abs_path(self, module: HadoopModule):
        return f"{self._hadoop_dir}/{module.value}"

    def copy_module_to_dist(self, module: HadoopModule):
        logger.info("Copying module %s", module.name)
        if module == HadoopModule.YARN_UI2:
            for module_path, dist_path in zip(glob.glob(f"{self._hadoop_dir}/{self.YARN_UI2_MODULE_PATH}"),
                                    glob.glob(f"{self._hadoop_dir}/{self.YARN_UI2_DIST_PATH}")):
                logger.info("Copying %s to %s", module_path, dist_path)
                dir_util.copy_tree(module_path, dist_path)

    def _find_jar(self, module: str) -> str:
        jar_cmd = RunnableCommand(self.FIND_JAR_OF_MODULE_TEMPLATE.format(module=module), work_dir=self._hadoop_dir)
        jar_cmd.run()
        if not jar_cmd.stdout:
            logger.warning("No jar found for module %s", module)
            logger.warning(jar_cmd.stderr)
            return ""

        jar_absolute = f"{self._hadoop_dir}/{jar_cmd.stdout[0]}"

        return jar_absolute

    def get_hadoop_dir(self) -> str:
        return self._hadoop_dir

    def add_modules(self, *args, with_jar=False):
        if not with_jar:
            [self._modules.__setitem__(m, "") for m in args]
        else:
            for m in args:
                self._modules[m] = self._find_jar(m)

    def get_jar_paths(self) -> Dict[str, str]:
        return self._modules

    def get_changed_jar_paths(self) -> Dict[str, str]:
        return self._changed

    def get_modules(self) -> List[str]:
        return list(self._modules.keys())

    def switch_branch_to(self, branch):
        logger.info("Switching branch to %s", branch)
        cmd = self.SWITCH_BRANCH_CMD_TEMPLATE.format(branch)
        br_cmd = RunnableCommand(cmd, work_dir=self._hadoop_dir)

        br_cmd.run()
        if not br_cmd.stdout and not br_cmd.stderr:
            out = "\n".join(br_cmd.stdout)
            err = "\n".join(br_cmd.stderr)
            msg = f"stdout: {out}\nstderr: {err}"
            raise CommandExecutionException(msg, cmd)

    def get_current_branch(self, fallback=None):
        br_cmd = RunnableCommand(self.GET_BRANCH_CMD, work_dir=self._hadoop_dir)
        br_cmd.run()
        if not br_cmd.stdout:
            raise CommandExecutionException("\n".join(br_cmd.stdout), br_cmd)
        res = br_cmd.stdout
        if isinstance(res, list):
            if len(res) == 1:
                res = res[0]
            else:
                raise HadesException("Cannot determine current git branch. Output of command: {}".format(res))
        if res == "HEAD":
            return fallback
        return res

    def reset(self, branch):
        logger.info("Resetting branch to %s", branch)
        cmd = self.RESET_HARD_CMD_TEMPLATE.format(branch)
        reset_cmd = RunnableCommand(cmd, work_dir=self._hadoop_dir)

        reset_cmd.run()
        if not reset_cmd.stdout:
            raise CommandExecutionException("\n".join(reset_cmd.stdout), cmd)

        cmd = "git clean -fd"
        git_clean_cmd = RunnableCommand(cmd, work_dir=self._hadoop_dir)
        git_clean_cmd.run()

    def apply_patch(self, patch_path, expected_branch="origin/trunk", force_reset=False):
        if force_reset:
            self.reset(expected_branch)
        else:
            self.switch_branch_to(expected_branch)
        logger.info("Applying patch file %s", patch_path)
        cmd = self.APPLY_PATCH_CMD_TEMPLATE.format(patch_path)
        patch_command = RunnableCommand(cmd, work_dir=self._hadoop_dir)

        patch_command.run()
