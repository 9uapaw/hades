import enum
from typing import Dict

import pptree as pptree

from core.error import HadesException
from hadoop.hadock.docker_host import DockerContainerInstance
from hadoop.role import HadoopRoleInstance, HadoopRoleType
import requests

from hadoop.yarn.cs_queue import CapacitySchedulerQueue


class HadoopAuthentication(enum.Enum):
    SIMPLE = "SIMPLE"
    SECURE = "KERBEROS"


class RmApi:
    PREFIX = "ws/v1/cluster"

    def __init__(self, rm: HadoopRoleInstance, authentication=HadoopAuthentication.SIMPLE):
        self._rm = rm
        self._authentication = authentication

    def get_metrics(self) -> Dict[str, any]:
        return self._get("metrics")['clusterMetrics']

    def get_scheduler_info(self) -> Dict[str, any]:
        return self._get("scheduler")

    def modify_config(self, config: str):
        return self._put("scheduler-conf", config)

    def _get(self, endpoint: str) -> Dict[any, any]:
        if self._authentication == HadoopAuthentication.SIMPLE:
            endpoint = endpoint + "?user.name=yarn"
        rm_host = self._get_rm_address()
        return requests.get("{}/{}/{}".format(rm_host, self.PREFIX, endpoint)).json()

    def _put(self, endpoint: str, data: str):
        if self._authentication == HadoopAuthentication.SIMPLE:
            endpoint = endpoint + "?user.name=yarn"
        headers = {'Content-Type': 'application/xml'}
        res = requests.put("{}/{}/{}".format(self._get_rm_address(), self.PREFIX, endpoint), data, headers=headers)
        if res.status_code < 200 or res.status_code >= 300:
            raise HadesException("Error while sending PUT request to {}. Cause: {}".format(res.url, res.text))

    def _get_rm_address(self):
        rm_host = str(self._rm.host.get_address()).split("://")
        if len(rm_host) == 1:
            rm_host = "http://{}:8088".format(self._rm.host.get_address())
        else:
            rm_host = str(self._rm.host) + ":8088"
        return rm_host


if __name__ == "__main__":
    rm_api = RmApi(HadoopRoleInstance("YARN-1", DockerContainerInstance("resourcemanager"), "resourcemanager", HadoopRoleType.RM))
    queues = rm_api.get_scheduler_info()
    q = CapacitySchedulerQueue.from_rm_api_data(queues)

    pptree.print_tree(q.get_root(), nameattr="")
