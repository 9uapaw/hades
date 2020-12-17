from typing import Dict

import pptree as pptree
import yaml

from hadoop.host import DockerContainerInstance
from hadoop.role import HadoopRoleInstance, HadoopRoleType
import requests

from hadoop.yarn.cs_queue import CapacitySchedulerQueue


class RmApi:
    PREFIX = "ws/v1/cluster"

    def __init__(self, rm: HadoopRoleInstance):
        self._rm = rm

    def get_metrics(self) -> Dict[str, any]:
        return self._get("metrics")['clusterMetrics']

    def get_queues(self) -> Dict[str, any]:
        return self._get("scheduler")

    def _get(self, endpoint: str) -> Dict[any, any]:
        rm_host = str(self._rm.host.get_host()).split("://")
        if len(rm_host) == 1:
            rm_host = "http://{}".format(self._rm.host.get_host())
        else:
            rm_host = str(self._rm.host)
        return requests.get("{}:8088/{}/{}".format(rm_host, self.PREFIX, endpoint)).json()


if __name__ == "__main__":
    rm_api = RmApi(HadoopRoleInstance("YARN-1", DockerContainerInstance("resourcemanager"), "resourcemanager", HadoopRoleType.RM))
    queues = rm_api.get_queues()
    q = CapacitySchedulerQueue.from_rm_api_data(queues)

    pptree.print_tree(q.get_root(), nameattr="")
