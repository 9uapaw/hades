import logging
import os
import time
from pprint import pformat
from typing import List, Type, Dict, Tuple

from pythoncommons.file_utils import FileUtils as CommonFileUtils

from core.cmd import RunnableCommand, DownloadCommand
from core.config import ClusterConfig, ClusterRoleConfig, ClusterContextConfig
from core.error import CommandExecutionException, MultiCommandExecutionException, HadesException
from hadoop.app.example import ApplicationCommand, MapReduceApp, DistributedShellApp
from hadoop.cluster import HadoopLogLevel
from hadoop.cluster_type import ClusterType
from hadoop.config import HadoopConfigBase
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.hadoop_config import HadoopConfigFile
from hadoop.host import HadoopHostInstance, RemoteHostInstance
from hadoop.role import HadoopRoleType, HadoopRoleInstance
from hadoop.yarn.nm_api import DEFAULT_NM_PORT
from hadoop.yarn.rm_api import RmApi, DEFAULT_RM_PORT
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
        # TODO Discover HDFS daemons
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
            if role.role_type == HadoopRoleType.RM:
                port = int(DEFAULT_RM_PORT)
            elif role.role_type == HadoopRoleType.NM:
                port = int(DEFAULT_NM_PORT)
            else:
                raise HadesException("Unexpected role type: {}".format(role.role_type))
            cmd = f"yarn daemonlog -setlevel `hostname`:{port} {package} {level.value}"
            cmds.append(role.host.create_cmd(cmd))

        return cmds

    def get_log_levels(self, *args: 'HadoopRoleInstance', packages: List[str]) -> Dict[str, List[RunnableCommand]]:
        cmds = {}
        for role in args:
            if role.role_type == HadoopRoleType.RM:
                port = int(DEFAULT_RM_PORT)
            elif role.role_type == HadoopRoleType.NM:
                port = int(DEFAULT_NM_PORT)
            else:
                raise HadesException("Unexpected role type: {}".format(role.role_type))

            for package in packages:
                cmd = f"yarn daemonlog -getlevel `hostname`:{port} {package}"
                if package not in cmds:
                    cmds[package] = []
                cmds[package].append(role.host.create_cmd(cmd))

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

    def update_config(self,
                      *args: HadoopRoleInstance,
                      config: HadoopConfigBase,
                      no_backup: bool = False,
                      workdir: str = ".",
                      allow_empty: bool = False):
        config_name, config_ext = config.file.split(".")

        for role in args:
            logger.info(f"Setting config {config.file} on {role.get_colorized_output()}")
            local_file = f"{config_name}-{role.host}-{int(time.time())}.{config_ext}"
            parent_dir = os.getcwd() if workdir == "." else workdir
            local_file_path = os.path.join(parent_dir, local_file)
            config_file_path = self.CONFIG_FILE_PATH.format(config.file)
            try:
                cmd = role.host.download(config_file_path, local_file_path)
                cmd.run()
            except CommandExecutionException as e:
                # TODO Decide if config is XML based
                exc_stderr = "\n".join(e.stderr)
                no_such_file_marker = "No such file or directory"
                if (no_such_file_marker in cmd.stderr or no_such_file_marker in exc_stderr) and allow_empty:
                    logger.warning("Config file '%s' not found on host '%s'", config_file_path, role.host.address)
                    CommonFileUtils.create_new_empty_file(local_file_path)
                    CommonFileUtils.write_to_file(local_file_path, "<configuration>\n</configuration>")
                else:
                    raise e

            config.set_base_config(local_file_path)
            config.merge()
            config.commit()

            role.host.upload(config.file, config_file_path).run()

            if no_backup:
                logger.info("Backup is turned off. Deleting file %s", local_file_path)
                os.remove(local_file_path)

    def restart_roles(self, *args: HadoopRoleInstance) -> List[RunnableCommand]:
        return [role.host.create_cmd("yarn --daemon stop {role} && yarn --daemon start {role}".format(
            role=role.role_type.value)) for role in args]

    def force_restart_roles(self, *args: HadoopRoleInstance, sleep_after: int = 0) -> None:
        for role in args:
            pid = self._get_pid_by_role(role)
            cmd = f"kill {pid} && sleep 15 && yarn --daemon start {role.role_type.value}"
            if sleep_after > 0:
                cmd += f" && sleep {sleep_after}"
            role.host.create_cmd(cmd).run()

    def get_role_pids(self, *args: 'HadoopRoleInstance'):
        result = {}
        for role in args:
            result[role] = self._get_pid_by_role(role)
        return result

    @staticmethod
    def _get_pid_by_role(role):
        try:
            out = role.host.create_cmd(f"jps | grep -i {role.role_type.value}").run()
            first_line = out[0]
            if len(first_line) > 1:
                raise HadesException("Unexpected output from jps: '{}'. Full output: {}".format(first_line, out))
            pid = first_line[0].split(" ")[0]
            return pid
        except CommandExecutionException as e:
            logger.exception("No %s process is running on host %s!", role.role_type.value, role.host)
            return None

    def restart_cluster(self, cluster: str):
        pass

    def get_config(self, *args: 'HadoopRoleInstance', config: HadoopConfigFile) -> Dict[str, HadoopConfigBase]:
        configs = {}
        config_name, config_ext = config.val.split(".")

        for role in args:
            config_data = HadoopConfigBase.create(config)
            local_file = f"{config_name}-{role.host}-{int(time.time())}.{config_ext}"
            config_file_path = self.CONFIG_FILE_PATH.format(config.val)
            role.host.download(config_file_path, local_file).run()

            config_data.set_base_config(local_file)
            config_data.merge()
            config_data.commit()

            os.remove(local_file)
            configs[role.name] = config_data

        return configs

    def replace_module_jars(self, *args: 'HadoopRoleInstance', modules: HadoopDir):
        uniqe_roles = {role.host.get_address(): role for role in args}
        cached_found_jar = {}
        for role in uniqe_roles.values():
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
                    # 0th item is stdout, 1st item is stderr
                    stdout = find_remote_jar[0]
                    if stdout:
                        if len(stdout) > 1:
                            logger.debug("Found ambiguous jars on host '%s' for module '%s': %s", role.host.get_address(), module, "\n".join(stdout))
                            # Find best match according to project version
                            module_dir = os.path.join(remote_jar_dir, module)
                            jar_path = f"{module_dir}-{modules.project_version}.jar"
                            if jar_path not in stdout:
                                raise HadesException("Found ambiguous jars on host '{}' for module '{}': {}.\n"
                                                     "Assumed jar path is not correct: {}", role.host.get_address(), module, "\n".join(stdout), jar_path)
                            remote_jar = jar_path
                        else:
                            remote_jar = stdout[0]
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

    def upload_file(self, *args: 'HadoopRoleInstance', local_file, target_path) -> None:
        for role in args:
            logger.info("Uploading local file '%s' on host '%s' to path '%s'", local_file, role.host, target_path)
            role.host.upload(local_file, target_path).run()

    def compile_java(self, *args: 'HadoopRoleInstance', file_path, target_dir):
        for role in args:
            logger.info("Compiling Java file '%s' on host '%s' to target dir '%s'", file_path, role.host, target_dir)
            copy_to_target_dir = f"mkdir -p {target_dir} && cp {file_path} {target_dir}"
            compile = f"cd {target_dir} && javac -d . *.java"
            role.host.create_cmd(f"{copy_to_target_dir} && {compile}").run()

    def execute_java(self, *args: 'HadoopRoleInstance', classpath: str, working_dir: str, main_class: str, program_args: List[str]):
        outputs = {}
        for role in args:
            logger.info("Executing Java main class '%s' (classpath: %s, working dir: %s) on host '%s'", main_class, classpath, working_dir, role.host)
            args_str = " ".join(program_args)
            cmd = f"cd {working_dir} && java -classpath {classpath} {main_class} {args_str}"
            cmd_obj = role.host.create_cmd(cmd)
            cmd_obj.run()
            if len(cmd_obj.stdout) != 2:
                raise HadesException("Expected a 2-line stdout from command '{}'. Output was: {}".format(cmd, cmd_obj.stdout))
            outputs[role.host] = cmd_obj.stdout[1]
        return outputs

    def generate_keypair(self, *args: 'HadoopRoleInstance', dname: str, keystore: str, store_pass: str, alias: str, extensions: List[str],
                         key_alg="RSA",
                         keysize=2048,
                         validity=3650
                         ):
        for i, role in enumerate(args):
            ext_args = ""
            for ext in extensions:
                if "$HOSTNAME" in ext:
                    ext = ext.replace("$HOSTNAME", role.host.address)
                ext_args += f"-ext {ext} "
            alias_arg = self._determine_alias_arg(alias, role)
            cmd_1 = f"keytool -v -genkeypair -dname \"{dname}\" -keystore {keystore} -storepass {store_pass} {alias_arg} " \
                    f"-keyalg {key_alg} -keysize {keysize} -validity {validity} {ext_args}"

            # Verify
            cmd_verify = f"keytool -list -keystore {keystore} -storepass {store_pass}"
            cmd = f"{cmd_1} && {cmd_verify}"
            cmd_obj = role.host.create_cmd(cmd)
            cmd_obj.run()
            StandardUpstreamExecutor._log_all(cmd_obj)

    def export_cert_from_keystore(self, *args: 'HadoopRoleInstance', dest_dir: str, dest_cert_ext: str, alias: str, keystore: str, store_pass: str):
        for i, role in enumerate(args):
            alias_arg = self._determine_alias_arg(alias, role)
            dest_cert_filename = f"{self._auto_generate_server_name(role)}.{dest_cert_ext}"

            cmd_makedir = f"mkdir -p {dest_dir} && rm {dest_dir}/*"
            cmd_exportcert = f"keytool -v -exportcert -file {dest_dir}/{dest_cert_filename} {alias_arg} -keystore {keystore} -storepass {store_pass} -rfc"
            cmd_verify = f"ls -la {dest_dir}"

            cmd = f"{cmd_makedir} && {cmd_exportcert} && {cmd_verify}"
            cmd_obj = role.host.create_cmd(cmd)
            cmd_obj.run()
            StandardUpstreamExecutor._log_all(cmd_obj)

    def scp_certs_from_other_hosts(self, *args: 'HadoopRoleInstance', src_dir: str, dest_dir: str, cert_ext: str, run_as_user: str = None):
        all_roles = set(args)
        for role in args:
            other_roles = set(all_roles)
            other_roles.remove(role)
            for other_role in other_roles:
                # TODO Harcoded username 'systest' -> is this a problem?
                hostname = other_role.host.address
                cert_filename = f"{self._auto_generate_server_name(other_role)}.{cert_ext}"
                src_filepath = f"{src_dir}/{cert_filename}"
                cmd_scp = ""
                if run_as_user:
                    cmd_scp += f"sudo -u {run_as_user} "
                cmd_scp += f"scp -o StrictHostKeyChecking=no systest@{hostname}:{src_filepath} {dest_dir}"
                cmd_verify = f"ls -la {dest_dir}"

                cmd = f"{cmd_scp} && {cmd_verify}"
                cmd_obj = role.host.create_cmd(cmd)
                cmd_obj.run()
                StandardUpstreamExecutor._log_all(cmd_obj)

    def import_certs(self, *args: 'HadoopRoleInstance', src_dir: str, filename_pattern: str, truststore: str, store_pass: str):
        for role in args:
            cmd_obj = role.host.create_cmd(f"find {src_dir}/{filename_pattern}")
            cmd_obj.run()
            cert_files = cmd_obj.stdout

            for cert_file in cert_files:
                alias = os.path.basename(cert_file)
                cmd_keytool = f"keytool -v -importcert -file {cert_file} -alias {alias} -keystore {truststore} -storepass {store_pass} -noprompt"
                cmd_verify = f"keytool -list -keystore {truststore} -storepass {store_pass}"
                cmd = f"{cmd_keytool} && {cmd_verify}"
                cmd_obj = role.host.create_cmd(cmd)
                cmd_obj.run()
                StandardUpstreamExecutor._log_all(cmd_obj)

    def _determine_alias_arg(self, alias, role):
        if alias == "autogenerated":
            alias = self._auto_generate_server_name(role)
        alias_arg = ""
        if alias:
            alias_arg = f"-alias {alias}"
        return alias_arg

    @staticmethod
    def _auto_generate_server_name(role):
        nums = list(filter(str.isdigit, role.host.address))
        if len(nums) == 1:
            return f"server-{nums[0]}"
        elif len(nums) == 2:
            return f"server-{nums[0]}_{nums[1]}"
        else:
            # Use last number as ID
            return f"server-{nums[-1]}"

    def modify_file_permissions(self, *args: 'HadoopRoleInstance', file, owner_group: str, permission: int):
        if ":" not in owner_group:
            raise ValueError("Expected the owner_group parameter in format: '<owner>:<group'")

        for role in args:
            role.host.create_cmd(f"chown {owner_group} {file}").run()
            role.host.create_cmd(f"chmod {permission} {file}").run()

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
            tar_cmd.run()
            StandardUpstreamExecutor._log_all(tar_cmd)
        except CommandExecutionException as e:
            # If any of the tar commands fail, raise the Exception
            raise e

        return targz_file_path, targz_file_name

    @staticmethod
    def _log_all(cmd: RunnableCommand):
        StandardUpstreamExecutor._log_stdout(cmd)
        StandardUpstreamExecutor._log_stderr(cmd)

    @staticmethod
    def _log_stdout(cmd: RunnableCommand, info=False):
        if info:
            logger.info(pformat(cmd.stdout))
        else:
            logger.debug("stdout: %s", pformat(cmd.stdout))

    @staticmethod
    def _log_stderr(cmd: RunnableCommand):
        logger.debug("stderr: %s", pformat(cmd.stderr))

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
            tar_cmd.run()
            StandardUpstreamExecutor._log_all(tar_cmd)
        except CommandExecutionException as e:
            # If any of the tar commands fail, raise the Exception
            raise e

        return targz_file_path, targz_file_name

    def cleanup_files(self, *args: 'HadoopRoleInstance', dirs: List[str], limit: int):
        for role in args:
            logger.info("Running cleanup on %s. Dirs: %s", role.host.address, dirs)
            size_descriptor = f"{limit}M"

            rm_cmd = "rm -rf "
            for dir in dirs:
                du_cmd = f"du -sh {dir}/* --block-size=1M | sort -rh"

                logger.debug("Cleaning up files in %s:%s", role.host.address, dir)
                show_large_files_cmd = role.host.create_cmd(du_cmd)
                show_large_files_cmd.run()
                StandardUpstreamExecutor._log_stdout(show_large_files_cmd, info=True)

                rm_large_files_find_cmd = role.host.create_cmd(f"find {dir} -type f -size {size_descriptor} -maxdepth 1 2>/dev/null")
                rm_large_files_find_cmd.run()

                du_output, _ = role.host.create_cmd(du_cmd).run()

                for line in du_output:
                    split = line.split("\t")
                    if len(split) != 2:
                        raise ValueError("Unexpected output line of du command. Should have 2 parts: size and file name. "
                                         "Actual line: '%s'", line)
                    size = split[0]
                    file = split[1]
                    if int(size) >= limit:
                        logger.info("Detected large file: %s\t%s", size, file)
                        rm_cmd += f"{file} "

            if rm_cmd != "rm -rf ":
                response = input("Are you sure you want to execute command: {}:{} ? ".format(role.host.address, rm_cmd))
                if response in ("y", "yes"):
                    rm_cmd_obj = role.host.create_cmd(rm_cmd)
                    rm_cmd_obj.run()
