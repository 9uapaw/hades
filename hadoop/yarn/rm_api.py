from typing import Dict, List

import requests

from core.error import HadesException
from hadoop.host import RemoteHostInstance
from hadoop.role import HadoopRoleInstance
from hadoop.yarn.api_common import HadoopAuthentication

DEFAULT_RM_PORT = "8088"


class RmApi:
    PREFIX = "ws/v1/cluster"

    def __init__(self, rm: HadoopRoleInstance, authentication=HadoopAuthentication.SIMPLE):
        self._rm = rm
        self._authentication = authentication

    def get_nodes(self) -> List[Dict[str, any]]:
        return self._get("nodes")['nodes']['node']

    def get_metrics(self) -> Dict[str, any]:
        return self._get("metrics")['clusterMetrics']

    def get_scheduler_info(self) -> Dict[str, any]:
        return self._get("scheduler")

    def validate_config(self, config: str):
        return self._post("scheduler-conf/validate", config)

    def modify_config(self, config: str):
        return self._put("scheduler-conf", config)

    def _get(self, endpoint: str) -> Dict[any, any]:
        if self._authentication == HadoopAuthentication.SIMPLE:
            endpoint = endpoint + "?user.name=yarn"
        rm_host = self._get_rm_address()
        return requests.get(f"{rm_host}/{self.PREFIX}/{endpoint}").json()

    def _put(self, endpoint: str, data: str):
        if self._authentication == HadoopAuthentication.SIMPLE:
            endpoint = endpoint + "?user.name=yarn"
        headers = {'Content-Type': 'application/xml'}
        rm_host = self._get_rm_address()
        res = requests.put(f"{rm_host}/{self.PREFIX}/{endpoint}", data, headers=headers)
        if res.status_code < 200 or res.status_code >= 300:
            raise HadesException(f"Error while sending PUT request to {res.url}. Cause: {res.text}")

    def _post(self, endpoint: str, data: str):
        if self._authentication == HadoopAuthentication.SIMPLE:
            endpoint = endpoint + "?user.name=yarn"
        headers = {'Content-Type': 'application/xml'}
        rm_host = self._get_rm_address()
        res = requests.post(f"{rm_host}/{self.PREFIX}/{endpoint}", data, headers=headers)
        if res.status_code < 200 or res.status_code >= 300:
            raise HadesException(f"Error while sending POST request to {res.url}. Cause: {res.text}")

    def _get_rm_address(self):
        rm_address = self._rm.host.get_address()
        rm_address.replace("http://", "")
        rm_host = rm_address
        port = DEFAULT_RM_PORT

        rm_address_with_port = rm_address.split(":")
        if len(rm_address_with_port) == 2:
            port = rm_address_with_port[1]
            rm_host = rm_address_with_port[0]

        rm_host = f"http://{rm_host}:{port}"

        return rm_host


if __name__ == "__main__":
    rm_api = RmApi(HadoopRoleInstance(RemoteHostInstance(None, "http://quasar-mfiwur-3.quasar-mfiwur.root.hwx.site:8090", "yarn"), "", None, None))
    print(rm_api.get_nodes())
