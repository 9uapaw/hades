import logging
import random
from typing import List, Callable, Dict

import hadoop.selector
from core.config import ClusterConfig
from hadoop.app.example import ApplicationCommand
from hadoop.cluster_type import ClusterType
from hadoop.config import HadoopConfig
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.role import HadoopRoleInstance, HadoopRoleType
from hadoop.service import HadoopService, YarnService, HdfsService
from hadoop.yarn.cs_queue import CapacitySchedulerQueue
from hadoop.yarn.rm_api import RmApi

logger = logging.getLogger(__name__)


class HadoopCluster:

    def __init__(self, executor: HadoopOperationExecutor, cluster_type: ClusterType, services: List[HadoopService], name: str = ''):
        self._services: List[HadoopService] = services
        self._executor: HadoopOperationExecutor = executor
        self._cluster_type = cluster_type
        self._name = name
        self._rm_api: RmApi or None = None
        rm_role = self._select_roles("Yarn/ResourceManager")
        if rm_role:
            self._rm_api = RmApi(rm_role[0])

    @classmethod
    def from_config(cls, config: ClusterConfig, executor: HadoopOperationExecutor) -> 'HadoopCluster':
        services = []

        for service_type, service in config.context.items():
            roles = {}
            for role_name, role in service.roles.items():
                host = executor.role_host_type()

                role = HadoopRoleInstance(service.name, host, role_name, HadoopRoleType(role.type))
                roles[role_name] = role

            if service_type.lower() == "yarn":
                service_obj = YarnService(executor, service.name, roles, config.cluster_name)
            elif service_type.lower() == 'hdfs':
                service_obj = HdfsService(executor, service.name, roles, config.cluster_name)
            else:
                continue

            services.append(service_obj)

        return HadoopCluster(executor, ClusterType(config.cluster_type), services, config.cluster_name)

    def get_services(self) -> List[HadoopService]:
        return self._services

    def read_logs(self, selector: str, follow: bool = False, tail: int or None = 10, grep: str = None):
        roles = self._select_roles(selector)
        if not roles:
            logger.warning("No roles found by selector '{}'".format(selector))

        cmds = self._executor.read_log(*roles, follow=follow, tail=tail)

        handlers = []
        for cmd in cmds:
            handlers = cmd.run_async(stdout=self._generate_role_output(logger, cmd.target, grep),
                                     stderr=self._generate_role_output(logger, cmd.target, grep), block=False)

        [p.wait() for p in handlers]

    def get_status(self) -> List[HadoopClusterStatusEntry]:
        return self._executor.get_cluster_status()

    def run_app(self, application: ApplicationCommand):
        selected = self._select_roles("")
        random_selected = selected[random.randint(0, len(selected) - 1)]

        self._executor.run_app(random_selected, application)

    def _select_roles(self, selector: str) -> List[HadoopRoleInstance]:
        selector_expr = hadoop.selector.HadoopRoleSelector(self.get_services())
        return selector_expr.select(selector)

    @staticmethod
    def _generate_role_output(logger: logging.Logger, target: HadoopRoleInstance, grep: str or None) -> Callable[[str], None]:
        return lambda line: logger.info("{} {}".format(target.get_colorized_output(), line.replace("\n", ""))) \
            if not grep or grep in line else ""

    def update_config(self, selector: str, config: HadoopConfig, no_backup: bool = False):
        selected = self._select_roles(selector)
        self._executor.update_config(*selected, config=config, no_backup=no_backup)

    def restart_roles(self, selector: str):
        selected = self._select_roles(selector)
        logger.info("Restarting roles {}".format(" ".join([s.get_colorized_output() for s in selected])))

        self._executor.restart_roles(*selected)

    def get_metrics(self) -> Dict[str, str]:
        return self._rm_api.get_metrics()

    def get_queues(self) -> CapacitySchedulerQueue:
        return CapacitySchedulerQueue.from_rm_api_data(self._rm_api.get_queues())

    def get_rm_api(self) -> RmApi:
        return self._rm_api
