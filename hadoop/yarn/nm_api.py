from typing import Dict

import requests

from core.error import HadesException
from hadoop.config import HadoopConfigBase
from hadoop.hadoop_config import HadoopConfigFile
from hadoop.host import RemoteHostInstance
from hadoop.role import HadoopRoleInstance
from hadoop.yarn.api_common import HadoopAuthentication, HadoopResponseFormat

DEFAULT_NM_PORT = "8042"


class NmApi:
    PREFIX = "ws/v1/cluster"

    def __init__(self, nm: HadoopRoleInstance, authentication=HadoopAuthentication.SIMPLE):
        self._nm = nm
        self._authentication = authentication

    def get_conf(self):
        return self._get("conf", with_prefix=False, resp_format=HadoopResponseFormat.XML)

    def _get(self, endpoint: str, with_prefix=True, resp_format=HadoopResponseFormat.JSON) -> Dict[any, any]:
        if self._authentication == HadoopAuthentication.SIMPLE:
            endpoint = endpoint + "?user.name=yarn"
        nm_host = self._get_nm_address()

        prefix = f"{self.PREFIX}/" if with_prefix else ""
        resp = requests.get(f"{nm_host}/{prefix}{endpoint}")

        if resp_format == HadoopResponseFormat.JSON:
            return resp.json()
        elif resp_format == HadoopResponseFormat.TEXT:
            return resp.text
        elif resp_format == HadoopResponseFormat.XML:
            hadoop_conf = HadoopConfigBase.create(HadoopConfigFile.YARN_SITE, None)
            hadoop_conf.set_xml_str(resp.text)
            return hadoop_conf.to_dict()

    def _put(self, endpoint: str, data: str):
        if self._authentication == HadoopAuthentication.SIMPLE:
            endpoint = endpoint + "?user.name=yarn"
        headers = {'Content-Type': 'application/xml'}
        nm_host = self._get_nm_address()
        res = requests.put(f"{nm_host}/{self.PREFIX}/{endpoint}", data, headers=headers)
        if res.status_code < 200 or res.status_code >= 300:
            raise HadesException(f"Error while sending PUT request to {res.url}. Cause: {res.text}")

    def _post(self, endpoint: str, data: str):
        if self._authentication == HadoopAuthentication.SIMPLE:
            endpoint = endpoint + "?user.name=yarn"
        headers = {'Content-Type': 'application/xml'}
        nm_host = self._get_nm_address()
        res = requests.post(f"{nm_host}/{self.PREFIX}/{endpoint}", data, headers=headers)
        if res.status_code < 200 or res.status_code >= 300:
            raise HadesException(f"Error while sending POST request to {res.url}. Cause: {res.text}")

    def _get_nm_address(self):
        nm_address = self._nm.host.get_address()
        nm_address.replace("http://", "")
        nm_host = nm_address
        port = DEFAULT_NM_PORT

        nm_address_with_port = nm_address.split(":")
        if len(nm_address_with_port) == 2:
            port = nm_address_with_port[1]
            nm_host = nm_address_with_port[0]

        nm_host = f"http://{nm_host}:{port}"

        return nm_host


if __name__ == "__main__":
    nm_api = NmApi(HadoopRoleInstance(RemoteHostInstance(None, "http://quasar-mfiwur-3.quasar-mfiwur.root.hwx.site:8090", "yarn"), "", None, None))
    print(nm_api.get_conf())
