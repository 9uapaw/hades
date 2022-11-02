import logging
import random
from enum import Enum
from typing import List, Dict

import hadoop.selector
from core.cmd import RunnableCommand, DownloadCommand
from core.config import ClusterConfig
from core.context import HadesContext
from core.error import HadesException
from hadoop.app.example import ApplicationCommand
from hadoop.cluster_type import ClusterType
from hadoop.config import HadoopConfigBase
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.hadoop_config import HadoopConfigFile
from hadoop.role import HadoopRoleInstance, HadoopRoleType
from hadoop.service import HadoopService, YarnService, HdfsService
from hadoop.yarn.cs_queue import CapacitySchedulerQueue
from hadoop.yarn.nm_api import NmApi
from hadoop.yarn.rm_api import RmApi
from hadoop_dir.module import HadoopDir
from local_dir.local_files import LocalFiles

KEY_STORE_GENERATOR_JAVA_CLUSTER_PATH = "/home/systest/JavaKeyStore.java"
KEY_STORE_GENERATOR_JAVA_COMPILED_CLASSES_DIR = "~systest/compiled_java/"

logger = logging.getLogger(__name__)


class HadoopLogLevel(Enum):
    INFO = "INFO"
    DEBUG = "DEBUG"


class HadoopCluster:

    def __init__(self, executor: HadoopOperationExecutor, cluster_type: ClusterType, services: List[HadoopService], context: HadesContext, name: str = ''):
        self._services: List[HadoopService] = services
        self._executor: HadoopOperationExecutor = executor
        self._cluster_type = cluster_type
        self.name = name
        self._rm_api: RmApi = None
        self._nm_apis: Dict[str, NmApi] = {}
        self.ctx = context

    @classmethod
    def from_config(cls, config: ClusterConfig, executor: HadoopOperationExecutor, context: HadesContext) -> 'HadoopCluster':
        cluster = HadoopCluster(executor, ClusterType(config.cluster_type), [], context, config.cluster_name)

        for service_type, service in config.context.items():
            if service_type.lower() == "yarn":
                service_obj = YarnService(executor, service.name, {}, None)
            elif service_type.lower() == 'hdfs':
                service_obj = HdfsService(executor, service.name, {}, None)
            else:
                continue

            for role_name, role in service.roles.items():
                host = executor.role_host_type(None, role.host, role.user)

                role = HadoopRoleInstance(host, role_name, HadoopRoleType(role.type), None)
                host.role = role
                service_obj.add_role(role)

            cluster.add_service(service_obj)

        cluster._create_rm_api()
        cluster._create_nm_api()
        return cluster

    def add_service(self, service: HadoopService):
        service.cluster = self
        self._services.append(service)

    def get_services(self) -> List[HadoopService]:
        return self._services

    def read_logs(self, selector: str, follow: bool = False, tail: int or None = 10, download: bool = None) -> List[RunnableCommand]:
        roles = self.select_roles(selector)
        if not roles:
            logger.warning("No roles found by selector '%s'", selector)
        logger.debug("Selected roles for read logs command: %s", roles)

        cmds = self._executor.read_log(*roles, follow=follow, tail=tail, download=download)

        return cmds

    def set_log_level(self, selector: str, package: str, log_level: HadoopLogLevel) -> List[RunnableCommand]:
        roles = self.select_roles(selector)
        if not roles:
            logger.warning("No roles found by selector '%s'", selector)
        logger.debug("Selected roles for setting log level: %s", roles)

        cmds = self._executor.set_log_level(*roles, package=package, level=log_level)
        return cmds

    def get_log_levels(self, selector: str, packages: List[str]) -> Dict[str, List[RunnableCommand]]:
        roles = self.select_roles(selector)
        if not roles:
            logger.warning("No roles found by selector '%s'", selector)
        logger.debug("Selected roles for setting log level: %s", roles)

        cmds = self._executor.get_log_levels(*roles, packages=packages)
        return cmds

    def compress_and_download_app_logs(self, selector: str, app_id: str, workdir: str = '.', compress_dir: bool = False) -> List[DownloadCommand]:
        roles = self.select_roles(selector)
        if not roles:
            logger.warning("No roles found by selector '%s'", selector)

        cmds = self._executor.compress_app_logs(*roles, app_id=app_id, workdir=workdir, compress_dir=compress_dir)
        return cmds

    def compress_and_download_daemon_logs(self, selector: str, workdir: str = '.') -> List[DownloadCommand]:
        roles = self.select_roles(selector)
        if not roles:
            logger.warning("No roles found by selector '%s'", selector)

        cmds = self._executor.compress_daemon_logs(*roles, workdir=workdir)
        return cmds

    def get_status(self) -> List[HadoopClusterStatusEntry]:
        return self._executor.get_cluster_status(self.name)

    def run_app(self, application: ApplicationCommand, selector: str = "") -> RunnableCommand:
        logger.info("Running app %s", application)
        random_selected = self._select_random_role(selector)

        return self._executor.run_app(random_selected, application)

    def select_roles(self, selector: str) -> List[HadoopRoleInstance]:
        selector_expr = hadoop.selector.HadoopRoleSelector(self.get_services())
        return selector_expr.select(selector)

    def update_config(self, selector: str, config: HadoopConfigBase, no_backup: bool = False, workdir: str = ".", allow_empty: bool = False):
        selected = self.select_roles(selector)
        self._executor.update_config(*selected, config=config, no_backup=no_backup, workdir=workdir, allow_empty=allow_empty)

    def restart_roles(self, selector: str) -> List[RunnableCommand]:
        selected = self.select_roles(selector)
        return self._executor.restart_roles(*selected) or []

    def force_restart_roles(self, selector: str) -> List[RunnableCommand]:
        selected = self.select_roles(selector)
        return self._executor.force_restart_roles(*selected) or []

    def restart(self):
        self._executor.restart_cluster(self.name)

    def restart_with_guarantee(self, selector: str):
        """
        Under some circumstances, Nodemanager not always stopped when command ran: 'yarn --daemon stop nodemanager'
        Verify that pids of NM processes are different after restart
        :param selector:
        :return:
        """

        handlers = []
        # Get pids before restart
        role_pids_before = self.get_role_pids(selector)

        # Do restart
        for cmd in self.restart_roles(selector):
            handlers.append(cmd.run_async())
        for h in handlers:
            h.wait()

        # Get pids after restart
        role_pids_after = self.get_role_pids(selector)

        # Compare pids
        same_pids = self._verify_if_pids_are_different(role_pids_after, role_pids_before)

        if same_pids:
            logger.warning(
                "pids of NodeManagers are the same before and after restart: %s. Trying to kill the processes and start NodeManagers.",
                same_pids)
        self.force_restart_roles(selector)

        # Check pids once again (after force restart)
        role_pids_after = self.get_role_pids(selector)
        same_pids = self._verify_if_pids_are_different(role_pids_after, role_pids_before)
        if same_pids:
            raise HadesException(
                "pids of NodeManagers are the same before and after restart (even after tried to kill them manually): {}".format(
                    same_pids))

    @staticmethod
    def _verify_if_pids_are_different(role_pids_after, role_pids_before):
        nm_hostnames = role_pids_before.keys()
        same_pids = {}
        for host in nm_hostnames:
            if role_pids_before[host] == role_pids_after[host]:
                same_pids[host] = role_pids_after[host]
        return same_pids

    def get_role_pids(self, selector: str):
        selected = self.select_roles(selector)
        return self._executor.get_role_pids(*selected) or []

    def get_role_pids_for_multi_selector(self, *selectors: str) -> List[RunnableCommand]:
        selected = [self.select_roles(s) for s in selectors]
        return self._executor.get_role_pids(*selected) or []

    def get_metrics(self) -> Dict[str, str]:
        return self._rm_api.get_metrics()

    def get_queues(self) -> CapacitySchedulerQueue:
        return CapacitySchedulerQueue.from_rm_api_data(self._rm_api.get_scheduler_info())

    def get_node_statuses(self, skipped_states: List[str] = None):
        if skipped_states is None:
            skipped_states = ["SHUTDOWN"]
        return list(filter(lambda n: n['state'] not in skipped_states, self._rm_api.get_nodes()))

    def get_state_and_health_report(self, exceptions=None):
        if not exceptions:
            exceptions = ["SHUTDOWN"]
        ns = self.get_node_statuses(skipped_states=exceptions)
        id_key = "id"
        fields = ["state", "healthReport"]

        ret = {}
        for n in ns:
            id = n[id_key]
            d = {}
            for f in fields:
                d[f] = n[f]
            ret[id] = d
        return ret

    def get_rm_api(self) -> RmApi:
        return self._rm_api

    def distribute(self, selector: str, source: str, dest: str):
        selected = self.select_roles(selector)
        for role in selected:
            logger.info("Distributing local file %s to remote host '%s' path %s", source, role.name, dest)
            role.host.make_backup(dest).run()
            role.host.upload(source, dest).run()

    def get_config(self, selector: str, config: HadoopConfigFile) -> Dict[str, HadoopConfigBase]:
        return self._executor.get_config(*self.select_roles(selector), config=config)

    def get_config_from_api(self, selector: str) -> Dict[str, Dict[str, str]]:
        nm_roles = self.select_roles(selector)
        ret = {}
        for nm_role in nm_roles:
            nm_api = self._nm_apis[nm_role.host.address]
            ret[nm_role.host.address] = nm_api.get_conf()
        return ret

    def replace_module_jars(self, selector: str, modules: HadoopDir):
        return self._executor.replace_module_jars(*self.select_roles(selector), modules=modules)

    def _create_rm_api(self):
        rm_role = self.select_roles("Yarn/ResourceManager")
        self._rm_api = RmApi(rm_role[0])

    def _create_nm_api(self):
        nm_roles = self.select_roles("Yarn/NodeManager")
        for nm_role in nm_roles:
            self._nm_apis[nm_role.host.address] = NmApi(nm_role)

    def _select_random_role(self, selector: str = "") -> HadoopRoleInstance:
        selected = self.select_roles(selector)
        return selected[random.randint(0, len(selected) - 1)]

    def get_running_apps(self, selector: str = "") -> RunnableCommand:
        random_selected = self._select_random_role(selector)
        return self._executor.get_running_apps(random_selected)

    def get_finished_apps(self, selector: str = "") -> RunnableCommand:
        random_selected = self._select_random_role(selector)
        return self._executor.get_finished_apps(random_selected)

    def generate_keystore(self,
                          selector: str,
                          store_type: str,
                          type: str,
                          target_path: str,
                          password: str):
        java_key_store = LocalFiles.get_unique_file("JavaKeyStore.java")
        self.upload_file(selector, java_key_store, KEY_STORE_GENERATOR_JAVA_CLUSTER_PATH)
        self.compile_java_file(selector, KEY_STORE_GENERATOR_JAVA_CLUSTER_PATH, KEY_STORE_GENERATOR_JAVA_COMPILED_CLASSES_DIR)
        keystore_files = self.execute_java(selector,
                                           classpath=".",
                                           working_dir=KEY_STORE_GENERATOR_JAVA_COMPILED_CLASSES_DIR,
                                           main_class="com.hades.keystore.JavaKeyStore",
                                           args=[type, target_path, password])

        def all_same(items):
            return all(x == items[0] for x in items)

        values = list(keystore_files.values())
        if not all_same(values):
            raise HadesException("Not all keystore file locations are the same for NodeManagers! Values are: {}".format(keystore_files))
        return values[0]

    def upload_file(self, selector: str, local_file: str, target_path: str) -> None:
        roles = self.select_roles(selector)
        return self._executor.upload_file(*roles, local_file=local_file, target_path=target_path)

    def compile_java_file(self, selector: str, file_path: str, target_dir: str) -> None:
        roles = self.select_roles(selector)
        return self._executor.compile_java(*roles, file_path=file_path, target_dir=target_dir)

    def execute_java(self, selector: str, classpath: str, working_dir: str, main_class: str, args: List[str] = []):
        roles = self.select_roles(selector)
        return self._executor.execute_java(*roles,
                                           classpath=classpath,
                                           working_dir=working_dir,
                                           main_class=main_class,
                                           program_args=args)
