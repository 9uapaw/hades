import logging
import time
import os
from typing import List, Type, Dict, Tuple

from core.cmd import RunnableCommand, DownloadCommand
from core.config import ClusterConfig, ClusterRoleConfig, ClusterContextConfig
from core.error import CommandExecutionException, MultiCommandExecutionException, HadesException
from hadoop.app.example import ApplicationCommand, MapReduceApp, DistributedShellApp
from hadoop.cluster import HadoopLogLevel
from hadoop.cluster_type import ClusterType
from hadoop.config import HadoopConfig
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.host import HadoopHostInstance, RemoteHostInstance
from hadoop.role import HadoopRoleType, HadoopRoleInstance
from hadoop.xml_config import HadoopConfigFile
from hadoop.yarn.rm_api import RmApi
from hadoop_dir.module import HadoopDir


logger = logging.getLogger(__name__)


class StandardUpstreamExecutor(HadoopOperationExecutor):
    JAR_DIR = "/opt/hadoop/share/hadoop/"
    LOG_DIR = "/opt/hadoop/logs"
    APP_LOG_DIR = "/tmp/hadoop-logs"
    CONFIG_FILE_PATH = "/opt/hadoop/etc/hadoop/{}"

    def __init__(self, rm_host: str):
        self._rm_host = rm_host

    @property
    def role_host_type(self) -> Type[HadoopHostInstance]:
        return RemoteHostInstance

    def discover(self) -> ClusterConfig:
        cluster = ClusterConfig(ClusterType.STANDARD.value)
        yarn_roles = {}
        hdfs_roles = {}
        role_instance = HadoopRoleInstance(None, "resourcemanager", None, None)
        rm_host = HadoopHostInstance(role_instance, self._rm_host, "yarn")
        role_instance.host = rm_host
        rm_api = RmApi(role_instance)

        rm_host_id = self._rm_host.replace("http://", "")
        rm_role_conf = ClusterRoleConfig()
        rm_role_conf.type = HadoopRoleType.RM
        rm_role_conf.host = rm_host_id
        yarn_roles[rm_host_id] = rm_role_conf

        for node in rm_api.get_nodes():
            role_conf = ClusterRoleConfig()
            role_conf.type = HadoopRoleType.NM
            role_conf.host = node['nodeHostName']
            yarn_roles[node['id']] = role_conf

        cluster.context['Yarn'] = ClusterContextConfig()
        cluster.context['Hdfs'] = ClusterContextConfig()
        cluster.context['Yarn'].name = 'Yarn'
        cluster.context['Yarn'].roles = yarn_roles
        cluster.context['Hdfs'].name = 'Hdfs'
        cluster.context['Hdfs'].roles = hdfs_roles

        return cluster

    def read_log(self, *args: HadoopRoleInstance, follow: bool = False, tail: int or None = 10, download: bool = None) -> List[RunnableCommand]:
        cmds = []
        for role in args:
            role_type = role.role_type.value

            file = f"{self.LOG_DIR}*/*{role_type}*log"
            if download:
                cmds.append(role.host.download(file))
                continue
            elif tail or follow:
                cmd = "tail -f {file}"
            else:
                cmd = "cat {file}"
            cmds.append(role.host.create_cmd(cmd.format(file=file)))

        return cmds

    def set_log_level(self, *args: 'HadoopRoleInstance', package: str, level: HadoopLogLevel) -> List[RunnableCommand]:
        cmds = []
        for role in args:
            cmd = f"yarn daemonlog -setlevel `hostname`:8088 {package} {level.value}"
            cmds.append(role.host.create_cmd(cmd))

        return cmds

    def compress_app_logs(self, *args: 'HadoopRoleInstance', app_id: str, workdir: str = '.', compress_dir: bool = False) -> List[DownloadCommand]:
        if app_id.startswith("application_"):
            app_id_no = app_id.split("application_")[1]
        else:
            app_id_no = app_id
        app_id_spec = f"application_{app_id_no}"
        logger.info("Looking for application with ID: '%s'", app_id_spec)

        find_failed_on_hosts: List[Tuple, CommandExecutionException] = []
        tar_files_created: Dict[HadoopRoleInstance, str] = {}
        no_of_hosts = len(args)

        # /tmp/hadoop-logs/application_1657557929851_0006_DEL_1658219238731/container_1657557929851_0006_01_000001
        # /tmp/hadoop-logs/application_1657557929851_0006_DEL_1658219238731/container_1657557929851_0006_01_000003
        # OR
        # /tmp/hadoop-logs/application_1657557929851_0006/container_1657557929851_0006_01_000001/
        find_cmd = f"find {self.APP_LOG_DIR} -name {app_id_spec} -print"
        cmds = []
        for role in args:
            try:
                find_results, stderr = role.host.create_cmd(find_cmd).run()
            except CommandExecutionException as e:
                # No need to raise immediately if files not found on a host
                find_failed_on_hosts.append((role, e))
                continue

            logger.debug("Find command: '%s' on host '%s', results: %s", find_cmd, role.host, find_results)
            # only run tar if this was successful and there are files found
            if not find_results:
                logger.warning("Find did not return files on host '%s'", role.host)
                continue

            targz_file_path, targz_file_name = self._create_tar_gz_on_host(app_id, role, find_results, compress_dir=compress_dir)
            tar_files_created[role] = targz_file_path
            parent_dir = os.getcwd() if workdir == '.' else workdir
            local_file_path = os.path.join(parent_dir, targz_file_name)
            download_command = role.host.download(source=targz_file_path, dest=local_file_path)
            cmds.append(download_command)

        if len(tar_files_created) == 0:
            hosts = [role.host for role in args]
            raise HadesException(f"Failed to compress app logs, no tar file created on any of the hosts: {hosts}")

        if no_of_hosts == len(find_failed_on_hosts):
            logger.error("Command '%s' failed on all hosts: %s", find_cmd, [r.host for r in args])
            for role, tar_file in tar_files_created.items():
                logger.debug("Removing file: %s from host '%s'", tar_file, role.host)
                role.host.create_cmd(f"rm {tar_file}").run()
            excs = [t[1] for t in find_failed_on_hosts]
            raise MultiCommandExecutionException(excs)
        return cmds

    def compress_daemon_logs(self, *args: 'HadoopRoleInstance', workdir: str = '.') -> List[DownloadCommand]:
        tar_files_created: Dict[HadoopRoleInstance, str] = {}

        cmds = []
        for role in args:
            targz_file_path, targz_file_name = self._create_tar_gz_of_dir_on_host(self.LOG_DIR, role)
            tar_files_created[role] = targz_file_path
            parent_dir = os.getcwd() if workdir == '.' else workdir
            local_file_path = os.path.join(parent_dir, targz_file_name)
            download_command = role.host.download(source=targz_file_path, dest=local_file_path)
            cmds.append(download_command)

        if len(tar_files_created) == 0:
            hosts = [role.host for role in args]
            raise HadesException(f"Failed to compress daemon logs, no tar file created on any of the hosts: {hosts}")

        return cmds

    def get_cluster_status(self, cluster_name: str = None) -> List[HadoopClusterStatusEntry]:
        raise NotImplementedError()

    def run_app(self, random_selected: HadoopRoleInstance, application: ApplicationCommand):
        application.path = "/opt/hadoop/share/hadoop/"
        if isinstance(application, MapReduceApp):
            application.path += "mapreduce"
        elif isinstance(application, DistributedShellApp):
            application.path += "yarn"

        application_command = application.build()
        full_command = f"/opt/hadoop/bin/{application_command}"
        cmd = random_selected.host.create_cmd(full_command)

        return cmd

    def update_config(self, *args: HadoopRoleInstance, config: HadoopConfig, no_backup: bool = False, workdir: str = "."):
        config_name, config_ext = config.file.split(".")

        for role in args:
            logger.info(f"Setting config {config.file} on {role.get_colorized_output()}")
            local_file = f"{config_name}-{role.host}-{int(time.time())}.{config_ext}"
            parent_dir = os.getcwd() if workdir == "." else workdir
            local_file_path = os.path.join(parent_dir, local_file)
            config_file_path = self.CONFIG_FILE_PATH.format(config.file)
            role.host.download(config_file_path, local_file_path).run()

            config.xml = local_file_path
            config.merge()
            config.commit()

            role.host.upload(config.file, config_file_path).run()

            if no_backup:
                logger.info("Backup is turned off. Deleting file %s", local_file_path)
                os.remove(local_file_path)

    def restart_roles(self, *args: HadoopRoleInstance) -> List[RunnableCommand]:
        return [role.host.create_cmd("yarn --daemon stop {role} && yarn --daemon start {role}".format(
            role=role.role_type.value)) for role in args]

    def force_restart_roles(self, *args: HadoopRoleInstance) -> None:
        for role in args:
            pid = self._get_pid_by_role(role)
            role.host.create_cmd(f"kill {pid} && sleep 15 && yarn --daemon start {role.role_type.value}").run()

    def get_role_pids(self, *args: 'HadoopRoleInstance'):
        result = {}
        for role in args:
            pid = self._get_pid_by_role(role)
            result[role.host.address] = pid
        return result

    @staticmethod
    def _get_pid_by_role(role):
        out = role.host.create_cmd(f"jps | grep -i {role.role_type.value}").run()
        first_line = out[0]
        if len(first_line) > 1:
            raise HadesException("Unexpected output from jps: '{}'. Full output: {}".format(first_line, out))
        pid = first_line[0].split(" ")[0]
        return pid

    def restart_cluster(self, cluster: str):
        pass

    def get_config(self, *args: 'HadoopRoleInstance', config: HadoopConfigFile) -> Dict[str, HadoopConfig]:
        configs = {}
        config_name, config_ext = config.value.split(".")

        for role in args:
            config_data = HadoopConfig(config)
            local_file = f"{config_name}-{role.host}-{int(time.time())}.{config_ext}"
            config_file_path = self.CONFIG_FILE_PATH.format(config.value)
            role.host.download(config_file_path, local_file).run()

            config_data.xml = local_file
            config_data.merge()
            config_data.commit()

            os.remove(local_file)
            configs[role.name] = config_data

        return configs

    def replace_module_jars(self, *args: 'HadoopRoleInstance', modules: HadoopDir):
        unique_args = {role.host.get_address(): role for role in args}
        cached_found_jar = {}
        for role in unique_args.values():
            logger.info("Replacing jars on %s", role.host.get_address())
            for module, jar in modules.get_jar_paths().items():
                logger.info("Replacing jar %s", jar)
                local_jar = jar
                if module not in cached_found_jar:
                    remote_jar_dir = self.JAR_DIR
                    if "yarn" in module:
                        remote_jar_dir += "yarn"
                    elif "mapreduce" in module:
                        remote_jar_dir += "mapreduce"

                    find_remote_jar = role.host.find_file(remote_jar_dir, f"*{module}*").run()
                    remote_jar = ""
                    if find_remote_jar[0]:
                        remote_jar = find_remote_jar[0][0]
                        cached_found_jar[module] = remote_jar
                else:
                    remote_jar = cached_found_jar[module]

                if remote_jar:
                    role.host.make_backup(remote_jar).run()
                    logger.info("Replacing remote jar %s:%s with local jar: %s", role.host, remote_jar, local_jar)
                    role.host.upload(local_jar, remote_jar).run()

    def get_running_apps(self, random_selected: HadoopRoleInstance):
        # https://stackoverflow.com/a/61321426/1106893
        # We don't want core.cmd.RunnableCommand.run to fail if grep's exit code is 1 when no result is found
        cmd = random_selected.host.create_cmd("yarn application -list 2>/dev/null | grep -oe application_[0-9]*_[0-9]* | sort -r || true")
        return cmd

    def get_finished_apps(self, random_selected: HadoopRoleInstance):
        # https://stackoverflow.com/a/61321426/1106893
        # We don't want core.cmd.RunnableCommand.run to fail if grep's exit code is 1 when no result is found
        cmd = random_selected.host.create_cmd("yarn application -list -appStates FINISHED 2>/dev/null | grep -oe application_[0-9]*_[0-9]* | sort -r || true")
        return cmd

    @staticmethod
    def _create_tar_gz_on_host(app_id: str, role: HadoopRoleInstance, files,
                               compress_dir: bool = False) -> Tuple[str, str]:
        logger.info("Creating targz of application logs %s on host %s", app_id, role.host.address)
        targz_file_name = f"{app_id}_{role.host}.tar.gz"
        targz_file_path = f"/tmp/{targz_file_name}"

        if compress_dir:
            # Assuming single result directory
            # Find command: 'find /tmp/hadoop-logs -name application_1658218984267_0087 -print'
            # results: ['/tmp/hadoop-logs/application_1658218984267_0087']
            # tar -czvf my_directory.tar.gz -C my_directory .
            if len(files) > 1:
                raise HadesException(f"Tried to compress directory with tar and assumed single result file set. Current files: {files}")

            app_data_dir = f"{app_id}_{role.host.address}"
            target_dir = f"/tmp/{app_data_dir}"
            role.host.create_cmd(f"rm -rf {target_dir} && mkdir {target_dir} && cp -R {files[0]} {target_dir}").run()
            tar_cmd = role.host.create_cmd(
                "tar -cvf {fname} -C {parent_dir} {dir}".format(fname=targz_file_path, parent_dir="/tmp", dir=f"./{app_data_dir}"))
        else:
            tar_cmd = role.host.create_cmd(
                "tar -cvf {fname} {files}".format(fname=targz_file_path, files=" ".join(files)))
        try:
            stdout, stderr = tar_cmd.run()
            logger.debug("stdout: %s", stdout)
            logger.debug("stderr: %s", stderr)
        except CommandExecutionException as e:
            # If any of the tar commands fail, raise the Exception
            raise e

        return targz_file_path, targz_file_name

    @staticmethod
    def _create_tar_gz_of_dir_on_host(dir: str, role: HadoopRoleInstance) -> Tuple[str, str]:
        logger.info("Creating targz of daemon logs in directory %s on host %s", dir, role.host.address)
        targz_file_name = f"{role.role_type.name}_daemonlogs_{role.host}.tar.gz"
        targz_file_path = f"/tmp/{targz_file_name}"

        logs_dir = f"{role.role_type.name}_daemonlogs_{role.host.address}"
        parent_dir = "/tmp"
        target_dir = f"{parent_dir}/{logs_dir}"
        role.host.create_cmd(f"rm -rf {target_dir}; mkdir {target_dir} && cp -R {dir}/* {target_dir}").run()
        tar_cmd = role.host.create_cmd(
            "tar -cvf {fname} -C {parent_dir} {dir}".format(fname=targz_file_path, parent_dir=parent_dir, dir=f"./{logs_dir}"))

        try:
            stdout, stderr = tar_cmd.run()
            logger.debug("stdout: %s", stdout)
            logger.debug("stderr: %s", stderr)
        except CommandExecutionException as e:
            # If any of the tar commands fail, raise the Exception
            raise e

        return targz_file_path, targz_file_name
