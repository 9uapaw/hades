import inspect
import logging
from os import path
from typing import Dict, List, Tuple, Type

from tabulate import tabulate

from core.config import Config
from core.context import HadesContext
from core.error import ConfigSetupException, HadesException
from format.blob import BlobFormat
from format.tree import TreeFormat
from hadoop.action import RoleAction
from hadoop.app.example import DistributedShellApp, Application, MapReduceApp
from hadoop.cluster import HadoopCluster
from hadoop.cluster_type import ClusterType
from hadoop.cm.cm_api import CmApi
from hadoop.cm.executor import CmExecutor
from hadoop.hadock.executor import HadockExecutor
from hadoop.xml_config import HadoopConfigFile
from hadoop_dir.module import HadoopDir, HadoopModules
from hadoop_dir.mvn import MavenCompiler
from script.base import HadesScriptBase

logger = logging.getLogger(__name__)


class MainCommandHandler:
    CM_HOST = 'host'
    CM_PASSWORD = 'password'
    CM_USERNAME = 'username'
    HADOCK_REPOSITORY = 'hadock_path'
    HADOCK_COMPOSE = 'hadock_compose'

    def __init__(self, ctx: HadesContext):
        self.ctx = ctx
        self.executor = None

        if not self.ctx:
            return

        if ctx.config.cluster.cluster_type.lower() == ClusterType.CM.value.lower():
            cm_api = CmApi(ctx.config.cluster.specific_context[self.CM_HOST],
                           ctx.config.cluster.specific_context[self.CM_USERNAME],
                           ctx.config.cluster.specific_context[self.CM_PASSWORD])
            self.executor = CmExecutor(cm_api)
        elif ctx.config.cluster.cluster_type.lower() == ClusterType.HADOCK.value.lower():
            if self.HADOCK_REPOSITORY not in ctx.config.cluster.specific_context:
                raise ConfigSetupException("hadockPath is not set in config")

            self.executor = HadockExecutor(ctx.config.cluster.specific_context[self.HADOCK_REPOSITORY],
                                           ctx.config.cluster.specific_context.get(self.HADOCK_COMPOSE))
        else:
            logger.warning("No executor is set")
            self.executor = None

    def init(self, config_path: str, cluster_type: ClusterType = None, cluster_specific: Dict[str, str] = None):
        config = None
        if path.exists(config_path):
            config = Config.from_file(config_path)
            if not cluster_type or not cluster_specific:
                logger.info("No action taken")
                return
        else:
            config = Config()

        hadock_path = cluster_specific.get('hadock_path', None)
        host = cluster_specific.get('host', None)
        username = cluster_specific.get('username', None)
        password = cluster_specific.get('password', None)

        executor = None
        if cluster_type:
            if cluster_type == ClusterType.HADOCK and hadock_path:
                executor = HadockExecutor(hadock_path, "docker-compose.yml")
                config.cluster.cluster_type = ClusterType.HADOCK.value
                config.cluster.specific_context = {'hadockPath': hadock_path}
            elif cluster_type == ClusterType.CM:
                if not host:
                    raise ConfigSetupException("CM host is not set")
                executor = CmExecutor(CmApi(host, username, password))
                config.cluster.cluster_type = ClusterType.CM.value
                config.cluster.specific_context = {'host': host, 'username': username, 'password': password}

        if executor:
            config.cluster = executor.discover()

        with open(config_path, 'w') as f:
            config_json = config.to_json()
            f.write(config_json)

        logger.info("Created config file {}".format(config_path))

    def compile(self, changed=False, deploy=False, modules=None, no_copy=False, single=None):
        if not self.ctx.config.hadoop_jar_path:
            raise ConfigSetupException("hadoopJarPath", "not set")

        mvn = MavenCompiler()
        hadoop_modules = HadoopDir(self.ctx.config.hadoop_path)

        if single:
            mvn.compile_single_module(hadoop_modules, single)
            hadoop_modules.copy_module_to_dist(single)
            return

        if modules:
            hadoop_modules.add_modules(*modules, with_jar=True)

        if changed and not modules:
            hadoop_modules.add_modules(*self.ctx.config.default_modules, with_jar=True)
            hadoop_modules.extract_changed_modules()

        logger.info("Found modules: {}".format(hadoop_modules.get_modules()))
        mvn.compile(hadoop_modules)

        if not no_copy:
            hadoop_modules.copy_modules_to_dist(self.ctx.config.hadoop_jar_path)

    def log(self, selector: str, follow: bool, tail: int, grep: str):
        cluster = self._create_cluster()
        cluster.read_logs(selector, follow, tail, grep)

    def print_status(self):
        cluster = self._create_cluster()
        status = cluster.get_status()
        table = [[s.name, s.status] for s in status]
        logger.info("Cluster status")
        logger.info("\n" + tabulate(table))

    def print_cluster_metrics(self):
        metrics = BlobFormat(self._create_cluster().get_metrics())
        logger.info("Cluster metrics")
        logger.info("\n" + metrics.format())

    def print_queues(self):
        queues = TreeFormat(self._create_cluster().get_queues().get_root())
        logger.info("Capacity Scheduler Queues")
        logger.info("\n" + queues.format())

    def _create_cluster(self) -> HadoopCluster:
        if not self.executor:
            raise ConfigSetupException("Can not create cluster without executor. Check config settings!")

        return HadoopCluster.from_config(self.ctx.config.cluster, self.executor)

    def run_app(self, app: str, cmd: str = None, queue: str = None):
        cluster = self._create_cluster()
        application = None
        if app.lower() == Application.DISTRIBUTED_SHELL.name.lower():
            application = DistributedShellApp(cmd=cmd, queue=queue)
        elif app.lower() == Application.MAPREDUCE.name.lower():
            application = MapReduceApp(cmd=cmd, queue=queue)

        cluster.run_app(application)

    def update_config(self, selector: str, file: HadoopConfigFile, properties: List[str], values: List[str],
                      no_backup: bool = False, source: str = None):
        cluster = self._create_cluster()
        cluster.update_config(selector, file, properties, values, no_backup, source)

    def role_action(self, selector: str, action: RoleAction):
        cluster = self._create_cluster()
        if action == RoleAction.RESTART:
            cluster.restart_roles(selector)

    def distribute(self, selector: str, files: Tuple[str]):
        pass

    def run_script(self, name: str):
        mod = __import__('script.{}'.format(name))
        script_module = getattr(mod, name, None)
        if not script_module:
            raise HadesException("Script {} not found".format(name))

        cls_members = inspect.getmembers(script_module, inspect.isclass)
        found_cls = None  # type: Type[HadesScriptBase]
        for (cls_name, cls) in cls_members:
            if cls.__base__ == HadesScriptBase:
                found_cls = cls

        if not found_cls:
            raise HadesException("No subclass of HadesScriptBase found in file {}".format(name))

        logger.info("Running script {} in file {}".format(found_cls.__name__, name))
        script = found_cls(self._create_cluster())
        script.run()
