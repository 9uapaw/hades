import logging
import time
import os
from typing import List, Type, Dict, Tuple

from core.cmd import RunnableCommand, DownloadCommand
from core.config import ClusterConfig, ClusterRoleConfig, ClusterContextConfig
from core.error import CommandExecutionException, MultiCommandExecutionException, HadesException
from hadoop.app.example import ApplicationCommand, MapReduceApp, DistributedShellApp
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
    JAR_DIR = "/opt/hadoop/share/"
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

            file = "{log_dir}*/*{role_type}*log".format(log_dir=self.LOG_DIR, role_type=role_type)
            if download:
                cmds.append(role.host.download(file))
                continue
            elif tail or follow:
                cmd = "tail -f {file}"
            else:
                cmd = "cat {file}"
            cmds.append(role.host.create_cmd(cmd.format(file=file)))

        return cmds

    def compress_app_logs(self, *args: 'HadoopRoleInstance', app_id: str) -> List[DownloadCommand]:
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
        find_cmd = "find {dir} -name {name} -print".format(dir=self.APP_LOG_DIR, name=app_id_spec)
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

            targz_file_path, targz_file_name = self._create_tar_gz_on_host(app_id, role, find_results)
            tar_files_created[role] = targz_file_path
            download_command = role.host.download(targz_file_path, local_file=targz_file_name)
            cmds.append(download_command)

        if len(tar_files_created) == 0:
            hosts = [role.host for role in args]
            raise HadesException("Failed to compress app logs, no tar file created on any of the hosts: {}".format(hosts))

        if no_of_hosts == len(find_failed_on_hosts):
            logger.error("Command '%s' failed on all hosts: %s", find_cmd, [r.host for r in args])
            for role, tar_file in tar_files_created.items():
                logger.debug("Removing file: %s from host '%s'", tar_file, role.host)
                role.host.create_cmd(f"rm {tar_file}").run()
            excs = [t[1] for t in find_failed_on_hosts]
            raise MultiCommandExecutionException(excs)
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
        full_command = "/opt/hadoop/bin/{}".format(application_command)
        cmd = random_selected.host.create_cmd(full_command)

        return cmd

    def update_config(self, *args: HadoopRoleInstance, config: HadoopConfig, no_backup: bool = False):
        config_name, config_ext = config.file.split(".")

        for role in args:
            logger.info("Setting config {} on {}".format(config.file, role.get_colorized_output()))
            local_file = "{config}-{host}-{time}.{ext}".format(
                host=role.host, config=config_name, time=int(time.time()), ext=config_ext)
            config_file_path = self.CONFIG_FILE_PATH.format(config.file)
            role.host.download(config_file_path, local_file).run()

            config.xml = local_file
            config.merge()
            config.commit()

            role.host.upload(config.file, config_file_path).run()

            if no_backup:
                logger.info("Backup is turned off. Deleting file {}".format(local_file))
                os.remove(local_file)

    def restart_roles(self, *args: HadoopRoleInstance) -> List[RunnableCommand]:
        return [role.host.create_cmd("yarn --daemon stop {role} && yarn --daemon start {role}".format(
            role=role.role_type.value)) for role in args]

    def restart_cluster(self, cluster: str):
        pass

    def get_config(self, *args: 'HadoopRoleInstance', config: HadoopConfigFile) -> Dict[str, HadoopConfig]:
        configs = {}
        config_name, config_ext = config.value.split(".")

        for role in args:
            config_data = HadoopConfig(config)
            local_file = "{config}-{container}-{time}.{ext}".format(
                container=role.host, config=config_name, time=int(time.time()), ext=config_ext)
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
            logger.info("Replacing jars on {}".format(role.host.get_address()))
            for module, jar in modules.get_jar_paths().items():
                logger.info("Replacing jar {}".format(jar))
                local_jar = jar
                if module not in cached_found_jar:
                    remote_jar_dir = self.JAR_DIR
                    if "yarn" in module:
                        remote_jar_dir += "yarn"
                    elif "mapreduce" in module:
                        remote_jar_dir += "mapreduce"

                    find_remote_jar = role.host.find_file(remote_jar_dir, "*{}*".format(module)).run()
                    remote_jar = ""
                    if find_remote_jar[0]:
                        remote_jar = find_remote_jar[0][0]
                        cached_found_jar[module] = remote_jar
                else:
                    remote_jar = cached_found_jar[module]

                if remote_jar:
                    role.host.make_backup(remote_jar).run()
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
    def _create_tar_gz_on_host(app_id: str, role: HadoopRoleInstance, files) -> Tuple[str, str]:
        targz_file_name = f"{app_id}_{role.host}.tar.gz"
        targz_file_path = f"/tmp/{targz_file_name}"
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