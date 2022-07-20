import logging
import random
from typing import List, Callable, Dict

import hadoop.selector
from core.cmd import RunnableCommand
from core.config import ClusterConfig
from core.context import HadesContext
from hadoop.app.example import ApplicationCommand
from hadoop.cluster_type import ClusterType
from hadoop.config import HadoopConfig
from hadoop.data.status import HadoopClusterStatusEntry, HadoopConfigEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.role import HadoopRoleInstance, HadoopRoleType
from hadoop.service import HadoopService, YarnService, HdfsService
from hadoop.xml_config import HadoopConfigFile
from hadoop.yarn.cs_queue import CapacitySchedulerQueue
from hadoop.yarn.rm_api import RmApi
from hadoop_dir.module import HadoopModule, HadoopDir

logger = logging.getLogger(__name__)


class HadoopCluster:

    def __init__(self, executor: HadoopOperationExecutor, cluster_type: ClusterType, services: List[HadoopService], context: HadesContext, name: str = ''):
        self._services: List[HadoopService] = services
        self._executor: HadoopOperationExecutor = executor
        self._cluster_type = cluster_type
        self.name = name
        self._rm_api: RmApi = None
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
        return cluster

    def add_service(self, service: HadoopService):
        service.cluster = self
        self._services.append(service)

    def get_services(self) -> List[HadoopService]:
        return self._services

    def read_logs(self, selector: str, follow: bool = False, tail: int or None = 10, download: bool = None) -> List[RunnableCommand]:
        roles = self.select_roles(selector)
        if not roles:
            logger.warning("No roles found by selector '{}'".format(selector))

        cmds = self._executor.read_log(*roles, follow=follow, tail=tail, download=download)

        return cmds

    def compress_and_download_app_logs(self, selector: str, app_id: str) -> List[RunnableCommand]:
        roles = self.select_roles(selector)
        if not roles:
            logger.warning("No roles found by selector '{}'".format(selector))

        cmds = self._executor.compress_app_logs(*roles, app_id=app_id)
        return cmds

    def get_status(self) -> List[HadoopClusterStatusEntry]:
        return self._executor.get_cluster_status(self.name)

    def run_app(self, application: ApplicationCommand, selector: str = "") -> RunnableCommand:
        logger.info("Running app {}".format(application.__class__.__name__))
        random_selected = self._select_random_role(selector)

        return self._executor.run_app(random_selected, application)

    def select_roles(self, selector: str) -> List[HadoopRoleInstance]:
        selector_expr = hadoop.selector.HadoopRoleSelector(self.get_services())
        return selector_expr.select(selector)

    def update_config(self, selector: str, config: HadoopConfig, no_backup: bool = False):
        selected = self.select_roles(selector)
        self._executor.update_config(*selected, config=config, no_backup=no_backup)

    def restart_roles(self, selector: str) -> List[RunnableCommand]:
        selected = self.select_roles(selector)
        return self._executor.restart_roles(*selected) or []

    def restart(self):
        self._executor.restart_cluster(self.name)

    def get_metrics(self) -> Dict[str, str]:
        return self._rm_api.get_metrics()

    def get_queues(self) -> CapacitySchedulerQueue:
        return CapacitySchedulerQueue.from_rm_api_data(self._rm_api.get_scheduler_info())

    def get_rm_api(self) -> RmApi:
        return self._rm_api

    def distribute(self, selector: str, source: str, dest: str):
        selected = self.select_roles(selector)
        for role in selected:
            logger.info("Distributing local file {} to remote host '{}' path {}".format(source, role.name, dest))
            role.host.make_backup(dest).run()
            role.host.upload(source, dest).run()

    def get_config(self, selector: str, config: HadoopConfigFile) -> Dict[str, HadoopConfig]:
        return self._executor.get_config(*self.select_roles(selector), config=config)

    def replace_module_jars(self, selector: str, modules: HadoopDir):
        return self._executor.replace_module_jars(*self.select_roles(selector), modules=modules)

    def _create_rm_api(self):
        rm_role = self.select_roles("Yarn/ResourceManager")
        self._rm_api = RmApi(rm_role[0])

    def _select_random_role(self, selector: str = "") -> HadoopRoleInstance:
        selected = self.select_roles(selector)
        return selected[random.randint(0, len(selected) - 1)]
