import enum
import glob
import logging
import distutils
from distutils import dir_util
from typing import Dict, List
import shutil

from core.cmd import RunnableCommand
from core.error import CommandExecutionException

logger = logging.getLogger(__name__)


class HadoopModule(enum.Enum):
    YARN_UI2 = "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-ui"
    HADOOP_DIST = "hadoop-dist"
    RESOURCEMANAGER = "hadoop-yarn-server-resourcemanager"


class HadoopDir:
    CHANGED_MODULES_CMD = "git status --porcelain | grep \".*hadoop.*\" | sed -E \"s/.*\\/(.*)\\/src.*/\\1/g\""
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

    def extract_changed_modules(self):
        logger.info("Searching modules in hadoop dir {}".format(self._hadoop_dir))
        module_cmd = RunnableCommand(self.CHANGED_MODULES_CMD, work_dir=self._hadoop_dir)

        module_cmd.run()
        if not module_cmd.stdout:
            raise CommandExecutionException("\n".join(module_cmd.stdout), self.CHANGED_MODULES_CMD)

        for module in set(module_cmd.stdout):
            self._modules[module] = self._find_jar(module)
            self._changed[module] = self._modules[module]

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

                full_path = "{}/{}".format(dest, new_path)
                logger.info("Copying {} to {}".format(original_jar, full_path))
                shutil.copy2(original_jar, full_path)

    def get_module_abs_path(self, module: HadoopModule):
        return "{}/{}".format(self._hadoop_dir, module.value)

    def copy_module_to_dist(self, module: HadoopModule):
        logger.info("Copying module {}".format(module.name))
        if module == HadoopModule.YARN_UI2:
            for module_path, dist_path in zip(glob.glob("{}/{}".format(self._hadoop_dir, self.YARN_UI2_MODULE_PATH)),
                                    glob.glob("{}/{}".format(self._hadoop_dir, self.YARN_UI2_DIST_PATH))):
                logger.info("Copying {} to {}".format(module_path, dist_path))
                dir_util.copy_tree(module_path, dist_path)

    def _find_jar(self, module: str) -> str:
        jar_cmd = RunnableCommand(self.FIND_JAR_OF_MODULE_TEMPLATE.format(module=module), work_dir=self._hadoop_dir)
        try:
            jar_cmd.run()
        except Exception as e:
            raise CommandExecutionException("Error while searching jar files",
                                            self.FIND_JAR_OF_MODULE_TEMPLATE.format(module=module))
        if not jar_cmd.stdout:
            logger.warning("No jar found for module {}".format(module))
            logger.warning(jar_cmd.stderr)
            return ""

        jar_absolute = "{}/{}".format(self._hadoop_dir, jar_cmd.stdout[0])

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
