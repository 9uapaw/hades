from typing import List, Dict

import cm_client
from cm_client import ApiRoleNameList, ApiConfigList, ApiCluster, ApiService, ApiRole, ApiConfig


class CmApi:

    def __init__(self, host: str, username: str = 'admin', password: str = 'admin', version="v40"):
        self._host = host
        self._username = username
        self._password = password

        cm_client.configuration.username = username
        cm_client.configuration.password = password
        cm_client.configuration.verify_ssl = False

        self.version = version

        self._api_url = host + '/api/' + self.version
        self._api_client = cm_client.ApiClient(self._api_url)
        self._cluster_api_instance = cm_client.ClustersResourceApi(self._api_client)
        self._service_api = cm_client.ServicesResourceApi(self._api_client)
        self._role_api = cm_client.RolesResourceApi(self._api_client)
        self._role_command_api = cm_client.RoleCommandsResourceApi(self._api_client)

    def get_clusters(self) -> List[ApiCluster]:
        return self._cluster_api_instance.read_clusters().items

    def get_services(self, cluster: str) -> List[ApiService]:
        return self._service_api.read_services(cluster).items

    def get_roles(self, cluster: str, service: str) -> List[ApiRole]:
        return self._role_api.read_roles(cluster, service).items

    def get_config(self, cluster: str, role: str, service: str) -> List[ApiConfig]:
        return self._role_api.read_role_config(cluster, role, service).items

    def update_config(self, cluster: str, role: str, service: str, config: Dict[str, str]):
        configs = []
        for key, value in config.items():
            configs.append(ApiConfig(name=key, value=value))
        body = ApiConfigList(items=configs)

        self._role_api.update_role_config(cluster, role, service, body=body)

    def restart_roles(self, cluster: str, service: str, *roles: str):
        roles = ApiRoleNameList(items=roles)
        self._role_command_api.restart_command(cluster, service, body=roles)

    def restart_cluster(self, cluster: str):
        self._cluster_api_instance.restart_command(cluster)


if __name__ == '__main__':
    cm = CmApi("http://gandras-1.gandras.root.hwx.site:7180")
    c = cm.get_clusters()
    s = cm.get_services(c[0].name)
    r = cm.get_roles(c[0].name, s[2].name)
    print(cm.get_config(c[0].name, r[4].name, s[2].name))
