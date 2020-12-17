import cm_client
from cm_client import ApiRoleNameList, ApiConfigList


class CmApi:
    API_VERSION = 'v40'

    def __init__(self, host: str, username: str = 'admin', password: str = 'admin'):
        self._host = host
        self._username = username
        self._password = password

        cm_client.configuration.username = username
        cm_client.configuration.password = password

        self._api_url = host + '/api/' + self.API_VERSION
        self._api_client = cm_client.ApiClient(self._api_url)
        self._cluster_api_instance = cm_client.ClustersResourceApi(self._api_client)
        self._service_api = cm_client.ServicesResourceApi(self._api_client)
        self._role_api = cm_client.RolesResourceApi(self._api_client)
        self._role_command_api = cm_client.RoleCommandsResourceApi(self._api_client)

    def get_clusters(self):
        return self._cluster_api_instance.read_clusters()
