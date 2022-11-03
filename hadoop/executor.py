from abc import ABC, abstractmethod
from typing import List, Type, Dict

from core.cmd import RunnableCommand, DownloadCommand
from core.config import ClusterConfig
from hadoop.app.example import ApplicationCommand
from hadoop.config import HadoopConfigBase
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.hadoop_config import HadoopConfigFile
from hadoop.host import HadoopHostInstance
from hadoop_dir.module import HadoopDir


class HadoopOperationExecutor(ABC):
    HDFS_SERVICES = ["namenode", "datanode"]

    @property
    @abstractmethod
    def role_host_type(self) -> Type[HadoopHostInstance]:
        raise NotImplementedError()

    @abstractmethod
    def discover(self) -> ClusterConfig:
        raise NotImplementedError()

    @abstractmethod
    def read_log(self, *args: 'HadoopRoleInstance', follow: bool = False, tail: int or None = 10, download: bool = None) -> List[RunnableCommand]:
        raise NotImplementedError()

    @abstractmethod
    def set_log_level(self, *args: 'HadoopRoleInstance', package: str, level: 'HadoopLogLevel') -> List[RunnableCommand]:
        raise NotImplementedError()

    @abstractmethod
    def get_log_levels(self, *args: 'HadoopRoleInstance', packages: List[str]) -> Dict[str, List[RunnableCommand]]:
        raise NotImplementedError()

    @abstractmethod
    def compress_app_logs(self, *args: 'HadoopRoleInstance', app_id: str, workdir: str = '.', compress_dir: bool = False) -> List[DownloadCommand]:
        raise NotImplementedError()

    @abstractmethod
    def compress_daemon_logs(self, *args: 'HadoopRoleInstance', workdir: str = '.', compress_dir: bool = False) -> List[DownloadCommand]:
        raise NotImplementedError()

    @abstractmethod
    def get_cluster_status(self, cluster_name: str = None) -> List[HadoopClusterStatusEntry]:
        # TODO Implement + Check if RM, NMs, JHS and all HDFS daemons are running!
        raise NotImplementedError()

    @abstractmethod
    def run_app(self, random_selected: 'HadoopRoleInstance', application: ApplicationCommand) -> RunnableCommand:
        raise NotImplementedError()

    @abstractmethod
    def update_config(self, *args: 'HadoopRoleInstance', config: HadoopConfigBase, no_backup: bool, workdir: str = ".", allow_empty: bool = False):
        raise NotImplementedError()

    @abstractmethod
    def restart_roles(self, *args: 'HadoopRoleInstance'):
        raise NotImplementedError()

    def force_restart_roles(self, *args: 'HadoopRoleInstance') -> None:
        pass

    @abstractmethod
    def get_role_pids(self, *args: 'HadoopRoleInstance'):
        raise NotImplementedError()

    @abstractmethod
    def restart_cluster(self, cluster: str):
        raise NotImplementedError()

    def get_config(self, *args: 'HadoopRoleInstance', config: HadoopConfigFile) -> Dict[str, HadoopConfigBase]:
        raise NotImplementedError()

    def replace_module_jars(self, *args: 'HadoopRoleInstance', modules: HadoopDir):
        raise NotImplementedError()

    def get_running_apps(self, *args: 'HadoopRoleInstance') -> RunnableCommand:
        raise NotImplementedError()

    def get_finished_apps(self, random_selected: 'HadoopRoleInstance'):
        raise NotImplementedError()

    def upload_file(self, *args: 'HadoopRoleInstance', local_file, target_path):
        pass

    def compile_java(self, *args: 'HadoopRoleInstance', file_path, target_dir):
        pass

    def execute_java(self, *args: 'HadoopRoleInstance', classpath: str, working_dir: str, main_class: str, program_args: List[str]):
        pass

    def generate_keypair(self, *args: 'HadoopRoleInstance', dname: str, keystore: str, store_pass: str, alias: str, extensions: List[str],
                         key_alg="RSA",
                         keysize=2048,
                         validity=3650):
        pass

    def modify_file_permissions(self, *args: 'HadoopRoleInstance', file, owner_group: str, permission: int):
        pass

    def export_cert_from_keystore(self, *args: 'HadoopRoleInstance', dest_dir: str, dest_cert_ext: str, alias: str, keystore: str, store_pass: str):
        pass

    def scp_certs_from_other_hosts(self, *args: 'HadoopRoleInstance', src_dir: str, dest_dir: str, cert_ext: str, run_as_user: str = None):
        pass

    def import_certs(self, *args: 'HadoopRoleInstance', src_dir: str, filename_pattern: str, truststore: str, store_pass: str):
        pass
