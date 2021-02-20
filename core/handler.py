import inspect
import logging
from os import path
from typing import Dict, List, Tuple, Type

from tabulate import tabulate

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
from hadoop.config import HadoopConfig
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
        self._cluster = None

        if not self.ctx:
            raise HadesException("No context is received")

        if not self.ctx.cluster_config or not self.ctx.cluster_config.cluster_type:
            return

        if ctx.cluster_config.cluster_type.lower() == ClusterType.CM.value.lower():
            cm_api = CmApi(ctx.cluster_config.specific_context[self.CM_HOST],
                           ctx.cluster_config.specific_context[self.CM_USERNAME],
                           ctx.cluster_config.specific_context[self.CM_PASSWORD])
            self.executor = CmExecutor(cm_api)
        elif ctx.cluster_config.cluster_type.lower() == ClusterType.HADOCK.value.lower():
            if self.HADOCK_REPOSITORY not in ctx.cluster_config.specific_context:
                raise ConfigSetupException("Hadock repository is not set")

            self.executor = HadockExecutor(ctx.cluster_config.specific_context[self.HADOCK_REPOSITORY],
                                           ctx.cluster_config.specific_context.get(self.HADOCK_COMPOSE))
        else:
            logger.warning("Unknown cluster type")
            self.executor = None



    def discover(self):
        if not self.executor:
            raise HadesException("No executor is set. Set cluster config.")

        if path.exists(self.ctx.cluster_config_path):
            logger.info("Cluster manifest already exists.")
            return

        with open(self.ctx.cluster_config_path, 'w') as f:
            cluster_config = self.executor.discover()
            cluster_config.specific_context = self.ctx.cluster_config.specific_context
            f.write(cluster_config.to_json())

        logger.info("Created cluster file {}".format(self.ctx.cluster_config_path))

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

        return HadoopCluster.from_config(self.ctx.cluster_config, self.executor)

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
        config = HadoopConfig(file)
        config.extend_with_args({k: v for k, v in zip(properties, values)})
        if source:
            config.extend_with_xml(source)

        cluster.update_config(selector, config, no_backup)

    def role_action(self, selector: str, action: RoleAction):
        cluster = self._create_cluster()
        if action == RoleAction.RESTART:
            cluster.restart_roles(selector)

    def distribute(self, selector: str, source: str, dest: str):
        self._create_cluster().distribute(selector, source, dest)

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
