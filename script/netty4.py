import itertools
import logging
import os.path
import pickle
import shutil
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, List, Dict, Tuple
import re

from tabulate import tabulate

from core.cmd import RunnableCommand
from core.error import ScriptException, HadesCommandTimedOutException, HadesException, CommandExecutionException
from core.handler import MainCommandHandler
from core.util import FileUtils, CompressedFileUtils, PrintUtils, StringUtils
from hadoop.app.example import MapReduceApp, MapReduceAppType
from hadoop.cluster import HadoopCluster, HadoopLogLevel
from hadoop.config import HadoopConfig, HadoopPropertiesConfig
from hadoop.role import HadoopRoleType
from hadoop.xml_config import HadoopConfigFile
from hadoop_dir.module import HadoopDir
from script.base import HadesScriptBase

DEFAULT_BRANCH = "origin/trunk"
LOG = logging.getLogger(__name__)

CONF_DIR_TC = "testcase_config"
CONF_DIR_INITIAL = "initial_config"
APP_ID_NOT_AVAILABLE = "N/A"

TIMEOUT_MSG_TEMPLATE = "Timed out after {} seconds"
ERROR_MSG_TEMPLATE = "Error after {} seconds"
UNLIMITED = 99999999
TC_LIMIT_UNLIMITED = UNLIMITED

YARN_SELECTOR = "Yarn"
NODEMANAGER_SELECTOR = "Yarn/NodeManager"
RESOURCEMANAGER_SELECTOR = "Yarn/ResourceManager"
NODE_TO_RUN_ON = "type=Yarn/name=nodemanager2"
MAPREDUCE_PREFIX = "mapreduce"
YARN_APP_MAPREDUCE_PREFIX = "yarn.app.mapreduce"
YARN_APP_MAPREDUCE_SHUFFLE_PREFIX = "yarn.app.mapreduce.shuffle"
MAPREDUCE_SHUFFLE_PREFIX = MAPREDUCE_PREFIX + ".shuffle"

CONF_DEBUG_DELAY = "yarn.nodemanager.delete.debug-delay-sec"
CONF_DISK_MAX_UTILIZATION = "yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage"
CONF_DISK_MAX_UTILIZATION_VAL = "99.5"

# START DEFAULT CONFIGS
SHUFFLE_MANAGE_OS_CACHE = MAPREDUCE_SHUFFLE_PREFIX + ".manage.os.cache"
SHUFFLE_MANAGE_OS_CACHE_DEFAULT = "true"

SHUFFLE_READAHEAD_BYTES = MAPREDUCE_SHUFFLE_PREFIX + ".readahead.bytes"
SHUFFLE_READAHEAD_BYTES_DEFAULT = 4 * 1024 * 1024

SHUFFLE_MAX_CONNECTIONS = MAPREDUCE_SHUFFLE_PREFIX + ".max.connections"
SHUFFLE_MAX_CONNECTIONS_DEFAULT = 0

SHUFFLE_MAX_THREADS = MAPREDUCE_SHUFFLE_PREFIX + "max.threads"
SHUFFLE_MAX_THREADS_DEFAULT = 0

SHUFFLE_TRANSFER_BUFFER_SIZE = MAPREDUCE_SHUFFLE_PREFIX + ".transfer.buffer.size"
SHUFFLE_TRANSFER_BUFFER_SIZE_DEFAULT = 128 * 1024

SHUFFLE_TRANSFERTO_ALLOWED = MAPREDUCE_SHUFFLE_PREFIX + ".transferTo.allowed"
SHUFFLE_TRANSFERTO_ALLOWED_DEFAULT = "true"

SHUFFLE_MAX_SESSION_OPEN_FILES = MAPREDUCE_SHUFFLE_PREFIX + ".max.session-open-files"
SHUFFLE_MAX_SESSION_OPEN_FILES_DEFAULT = 3

SHUFFLE_LISTEN_QUEUE_SIZE = MAPREDUCE_SHUFFLE_PREFIX + ".listen.queue.size"
SHUFFLE_LISTEN_QUEUE_SIZE_DEFAULT = 128

SHUFFLE_PORT = MAPREDUCE_PREFIX + ".port"
SHUFFLE_PORT_DEFAULT = 13562

SHUFFLE_SSL_FILE_BUFFER_SIZE = MAPREDUCE_SHUFFLE_PREFIX + ".ssl.file.buffer.size"
SHUFFLE_SSL_FILE_BUFFER_SIZE_DEFAULT = 60 * 1024

SHUFFLE_CONNECTION_KEEPALIVE_ENABLE = MAPREDUCE_SHUFFLE_PREFIX + ".connection-keep-alive.enable"
SHUFFLE_CONNECTION_KEEPALIVE_ENABLE_DEFAULT = "false"

SHUFFLE_CONNECTION_KEEPALIVE_TIMEOUT = MAPREDUCE_SHUFFLE_PREFIX + ".connection-keep-alive.timeout"
SHUFFLE_CONNECTION_KEEPALIVE_TIMEOUT_DEFAULT = 5

SHUFFLE_MAPOUTPUT_INFO_META_CACHE_SIZE = MAPREDUCE_SHUFFLE_PREFIX + ".mapoutput-info.meta.cache.size"
SHUFFLE_MAPOUTPUT_INFO_META_CACHE_SIZE_DEFAULT = 1000

SHUFFLE_SSL_ENABLED = MAPREDUCE_SHUFFLE_PREFIX + ".ssl.enabled"
SHUFFLE_SSL_ENABLED_DEFAULT = "false"

SHUFFLE_PATHCACHE_EXPIRE_AFTER_ACCESS_MINUTES = MAPREDUCE_SHUFFLE_PREFIX + ".pathcache.expire-after-access-minutes"
SHUFFLE_PATHCACHE_EXPIRE_AFTER_ACCESS_MINUTES_DEFAULT = 5

SHUFFLE_PATHCACHE_CONCURRENCY_LEVEL = MAPREDUCE_SHUFFLE_PREFIX + ".pathcache.concurrency-level"
SHUFFLE_PATHCACHE_CONCURRENCY_LEVEL_DEFAULT = 16

SHUFFLE_PATHCACHE_MAX_WEIGHT = MAPREDUCE_SHUFFLE_PREFIX + ".pathcache.max-weight"
SHUFFLE_PATHCACHE_MAX_WEIGHT_DEFAULT = 10 * 1024 * 1024

SHUFFLE_LOG_SEPARATE = YARN_APP_MAPREDUCE_SHUFFLE_PREFIX + ".log.separate"
SHUFFLE_LOG_SEPARATE_DEFAULT = "true"

SHUFFLE_LOG_LIMIT_KB = YARN_APP_MAPREDUCE_SHUFFLE_PREFIX + ".log.limit.kb"
SHUFFLE_LOG_LIMIT_KB_DEFAULT = 0

SHUFFLE_LOG_BACKUPS = YARN_APP_MAPREDUCE_SHUFFLE_PREFIX + ".log.backups"
SHUFFLE_LOG_BACKUPS_DEFAULT = 0

# END OF DEFAULT CONFIGS

DEFAULT_CONFIGS = {
    SHUFFLE_MANAGE_OS_CACHE: SHUFFLE_MANAGE_OS_CACHE_DEFAULT,
    SHUFFLE_READAHEAD_BYTES: SHUFFLE_READAHEAD_BYTES_DEFAULT,
    SHUFFLE_MAX_CONNECTIONS: SHUFFLE_MAX_CONNECTIONS_DEFAULT,
    SHUFFLE_MAX_THREADS: SHUFFLE_MAX_THREADS_DEFAULT,
    SHUFFLE_TRANSFER_BUFFER_SIZE: SHUFFLE_TRANSFER_BUFFER_SIZE_DEFAULT,
    SHUFFLE_TRANSFERTO_ALLOWED: SHUFFLE_TRANSFERTO_ALLOWED_DEFAULT,
    SHUFFLE_MAX_SESSION_OPEN_FILES: SHUFFLE_MAX_SESSION_OPEN_FILES_DEFAULT,
    SHUFFLE_LISTEN_QUEUE_SIZE: SHUFFLE_LISTEN_QUEUE_SIZE_DEFAULT,
    SHUFFLE_PORT: SHUFFLE_PORT_DEFAULT,
    SHUFFLE_SSL_FILE_BUFFER_SIZE: SHUFFLE_SSL_FILE_BUFFER_SIZE_DEFAULT,
    SHUFFLE_CONNECTION_KEEPALIVE_ENABLE: SHUFFLE_CONNECTION_KEEPALIVE_ENABLE_DEFAULT,
    SHUFFLE_CONNECTION_KEEPALIVE_TIMEOUT: SHUFFLE_CONNECTION_KEEPALIVE_TIMEOUT_DEFAULT,
    SHUFFLE_MAPOUTPUT_INFO_META_CACHE_SIZE: SHUFFLE_MAPOUTPUT_INFO_META_CACHE_SIZE_DEFAULT,
    SHUFFLE_SSL_ENABLED: SHUFFLE_SSL_ENABLED_DEFAULT,
    SHUFFLE_PATHCACHE_EXPIRE_AFTER_ACCESS_MINUTES: SHUFFLE_PATHCACHE_EXPIRE_AFTER_ACCESS_MINUTES_DEFAULT,
    SHUFFLE_PATHCACHE_CONCURRENCY_LEVEL: SHUFFLE_PATHCACHE_CONCURRENCY_LEVEL_DEFAULT,
    SHUFFLE_PATHCACHE_MAX_WEIGHT: SHUFFLE_PATHCACHE_MAX_WEIGHT_DEFAULT,
    SHUFFLE_LOG_SEPARATE: SHUFFLE_LOG_SEPARATE_DEFAULT,
    SHUFFLE_LOG_LIMIT_KB: SHUFFLE_LOG_LIMIT_KB_DEFAULT,
    SHUFFLE_LOG_BACKUPS: SHUFFLE_LOG_BACKUPS_DEFAULT,
}

DEFAULT_CORE_SITE_CONFIGS = {
    "hadoop.ssl.require.client.cert": "false",
    "hadoop.ssl.keystores.factory.class": "org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory",
    "hadoop.ssl.server.conf": "ssl-server.xml"
}

DEFAULT_SSL_SERVER_CONFIGS = {
    "ssl.server.keystore.type": "jks",
    "ssl.server.keystore.password": "abc123",
    "ssl.server.keystore.location": "REPLACETHIS"
}

APP_LOG_FILE_NAME_FORMAT = "app_{app}.log"
YARN_LOG_FILE_NAME_FORMAT = "{host}_{role}_{app}.log"
PREFIXED_YARN_LOG_FILE_NAME_FORMAT = "{prefix}_{host}_{role}.log"
YARN_LOG_FORMAT = "{name} - {log}"
CONF_FORMAT = "{host}_{conf}.xml"


def _callback(cmd: RunnableCommand, logs_dict: Dict[RunnableCommand, List[str]]) -> Callable:
    def _cb(line: str):
        if cmd not in logs_dict or not logs_dict[cmd]:
            logs_dict[cmd] = []
        logs_dict[cmd].append(YARN_LOG_FORMAT.format(name=cmd.target.host, log=line))
        # LOG.debug("****logs_dict: %s", logs_dict)
    return _cb


class Netty4TestcasesBuilder:
    def __init__(self, name):
        self.configs: Dict[str, List[str]] = {}
        self.name = name
        self.apps: List[MapReduceAppType] = []

    def with_config(self, conf_key: str, value: str):
        if conf_key not in self.configs:
            self.configs[conf_key] = [value]
        else:
            self.configs[conf_key].append(value)
        return self

    def with_configs(self, conf_key: str, values: List[str]):
        if conf_key in self.configs:
            LOG.warning("Overwriting config key '%s'", conf_key)
        self.configs[conf_key] = values
        return self

    def with_apps(self, *apps):
        self.apps = list(*apps)
        return self

    def generate_testcases(self, config):
        if not self.apps:
            raise ValueError(f"No apps defined for testcase: {self.name}")
        testcases = []
        conf_key_prefixed_list = []
        for conf_key, values in self.configs.items():
            conf_key_prefixed_list.append([conf_key + "_" + v for v in values])
        product = itertools.product(*conf_key_prefixed_list)
        tc_counter = 0
        for tup in product:
            conf_changes = {}
            for s in tup:
                conf_name, conf_value = s.split("_")
                conf_changes[conf_name] = conf_value
                tc_counter += 1
            for app_type in self.apps:
                testcases.append(Netty4Testcase(self.name, self._generate_tc_name(tc_counter, app_type), conf_changes, config.mr_apps[app_type]))
        return testcases

    def _generate_tc_name(self, tc_counter, app_type: MapReduceAppType):
        return f"{self.name}_{str(tc_counter)}_{app_type.value}"


@dataclass
class Netty4Testcase:
    simple_name: str
    name: str
    config_changes: Dict[str, str]
    app: MapReduceApp

    def __hash__(self):
        return hash(self.name)


class TestcaseResultType(Enum):
    FAILED = "error"
    TIMEOUT = "timed out"
    PASSED = "passed"


@dataclass
class LogVerification:
    role_type: HadoopRoleType
    text: str
    inverted_mode: bool = False


@dataclass
class TestcaseResult:
    type: TestcaseResultType
    app_command: RunnableCommand
    details: str = None


class OutputFileWriter:
    def __init__(self, cluster):
        self.cluster = cluster
        self._generated_files = GeneratedOutputFiles()
        self.tc_no = 0
        self.workdir = None
        self.context = None
        self.tc = None
        self.current_ctx_dir = None
        self.current_tc_dir = None

    def update_with_context_and_testcase(self, workdir, context, testcase):
        self.workdir = workdir
        self.context = context
        self.tc_no = self.tc_no + 1
        self.tc = testcase
        self._generated_files.update_with_ctx_and_tc(self.context, self.tc)
        LOG.info("Using workdir: %s", self.workdir)

        self.current_ctx_dir = os.path.join(self.workdir, self.context.get_dirname)
        if not os.path.exists(self.current_ctx_dir):
            os.mkdir(self.current_ctx_dir)

        self.current_tc_dir = os.path.join(self.workdir, self.context.get_dirname, f"tc{self.tc_no}_{self.tc.name}")
        if not os.path.exists(self.current_tc_dir):
            os.mkdir(self.current_tc_dir)
        LOG.debug("Current context dir is: %s", self.current_ctx_dir)
        LOG.debug("Current testcase dir is: %s", self.current_tc_dir)

    def _get_testcase_targz_filename(self):
        tc_no = f"0{str(self.tc_no)}" if self.tc_no < 9 else str(self.tc_no)
        tc_targz_filename = os.path.join(self.current_ctx_dir, f"testcase_{tc_no}_{self.tc.name}.tar.gz")
        return tc_targz_filename

    def _write_config_files(self, selector: str, conf_type: HadoopConfigFile, dir=None) -> List[str]:
        conf_type_str = ""
        if dir == CONF_DIR_INITIAL:
            conf_type_str = "initial"
        elif dir == CONF_DIR_TC:
            conf_type_str = "testcase"
        LOG.info("Writing initial %s files for selector '%s'", conf_type_str, selector)

        configs = self.cluster.get_config(selector, conf_type)
        generated_config_files = []
        for host, conf in configs.items():
            config_file_name = CONF_FORMAT.format(host=host, conf=conf_type.name)
            if dir:
                dir_path = os.path.join(self.current_tc_dir, dir)
                if not os.path.exists(dir_path):
                    os.mkdir(dir_path)
                file_path = os.path.join(dir_path, config_file_name)
            else:
                file_path = os.path.join(self.current_tc_dir, config_file_name)

            LOG.debug("Writing config file '%s' on host '%s'", file_path, host)
            with open(file_path, 'w') as f:
                f.write(conf.to_str())
            generated_config_files.append(file_path)
        return generated_config_files

    def write_initial_config_files(self):
        configs: List[Tuple[OutputFileType, HadoopConfigFile]] = [
            (OutputFileType.INITIAL_CONFIG_MR, HadoopConfigFile.MAPRED_SITE),
            (OutputFileType.INITIAL_CONFIG_YARN_SITE, HadoopConfigFile.YARN_SITE),
            (OutputFileType.INITIAL_CONFIG_CORE_SITE, HadoopConfigFile.CORE_SITE),
            (OutputFileType.INITIAL_CONFIG_SSL_SERVER, HadoopConfigFile.SSL_SERVER),
            (OutputFileType.INITIAL_CONFIG_LOG4J_PROPERTIES, HadoopConfigFile.LOG4J_PROPERTIES),
        ]

        for config_tup in configs:
            self._generated_files.register_files(config_tup[0],
                                                 self._write_config_files(NODEMANAGER_SELECTOR,
                                                                          config_tup[1],
                                                                          dir=CONF_DIR_INITIAL))

    def write_testcase_config_files(self):
        configs: List[Tuple[OutputFileType, HadoopConfigFile]] = [
            (OutputFileType.TC_CONFIG_MR, HadoopConfigFile.MAPRED_SITE),
            (OutputFileType.TC_CONFIG_YARN_SITE, HadoopConfigFile.YARN_SITE),
            (OutputFileType.TC_CONFIG_CORE_SITE, HadoopConfigFile.CORE_SITE),
            (OutputFileType.TC_CONFIG_SSL_SERVER, HadoopConfigFile.SSL_SERVER),
            (OutputFileType.TC_CONFIG_LOG4J_PROPERTIES, HadoopConfigFile.LOG4J_PROPERTIES),
        ]

        for config_tup in configs:
            self._generated_files.register_files(config_tup[0],
                                                 self._write_config_files(NODEMANAGER_SELECTOR,
                                                                          config_tup[1],
                                                                          dir=CONF_DIR_TC))

    def _write_role_logs(self, logs_by_role: 'LogsByRoles', prefix: str = "", app: str = ""):
        return self._write_yarn_log_file(logs_by_role.log_lines_dict, prefix=prefix, app=app)

    def _write_yarn_log_file(self, log_lines, prefix: str = "", app: str = ""):
        files_written = []
        if not log_lines:
            raise HadesException("YARN log lines dictionary is empty!")
        if not prefix and not app:
            raise HadesException("Either prefix or app should be specified for this method!")

        for cmd, lines in log_lines.items():
            if prefix:
                yarn_log_file = PREFIXED_YARN_LOG_FILE_NAME_FORMAT.format(prefix=prefix,
                                                                          host=cmd.target.host,
                                                                          role=cmd.target.role_type.name)
            else:
                yarn_log_file = YARN_LOG_FILE_NAME_FORMAT.format(host=cmd.target.host,
                                                                 role=cmd.target.role_type.name,
                                                                 app="YARN")
            file_path = os.path.join(self.current_tc_dir, yarn_log_file)
            LOG.info("Writing YARN log file: %s", file_path)
            files_written.append(file_path)
            with open(file_path, 'w') as f:
                f.writelines(lines)
        return files_written

    def _write_node_health_reports(self, node_health_reports):
        files_written = []
        if not node_health_reports:
            raise HadesException("YARN Node health report is empty for all nodes!")

        for node_id, dict in node_health_reports.items():
            chars = [(".", "_"), (":", "_"), ("-", "_")]
            new_node_id = StringUtils.replace_chars(node_id, chars)
            file_path = os.path.join(self.current_tc_dir, f"healthreport_{new_node_id}.txt")
            files_written.append(file_path)
            with open(file_path, 'w') as f:
                f.writelines(dict)
        return files_written

    def write_node_health_reports(self, node_health_reports):
        self._generated_files.register_files(OutputFileType.NODE_HEALTH_REPORTS,
                                             self._write_node_health_reports(node_health_reports))

    def write_nm_restart_logs(self, nm_restart_logs):
        self._generated_files.register_files(OutputFileType.NM_RESTART_LOGS,
                                             self._write_role_logs(nm_restart_logs, prefix="restart-", app="YARN"))

    def write_yarn_logs(self, yarn_logs):
        self._generated_files.register_files(OutputFileType.YARN_DAEMON_LOGS,
                                             self._write_role_logs(yarn_logs, prefix="", app="YARN"))

    def print_all_generated_files(self):
        self._generated_files.print_all()

    def print_all_generated_files_for_current_ctx(self):
        self._generated_files.print_all_for_current_ctx()

    def create_compressed_testcase_data_file(self, using_custom_workdir: bool = False):
        target_targz_file = self._get_testcase_targz_filename()
        if using_custom_workdir:
            FileUtils.compress_dir(filename=target_targz_file,
                                   dir=self.current_tc_dir)
        else:
            FileUtils.compress_files(filename=target_targz_file,
                                     files=self._generated_files.get_all_for_current_ctx())
        self._generated_files.register_files(OutputFileType.ALL_FILES_TAR_GZ, [target_targz_file])
        FileUtils.rm_dir(self.current_tc_dir)

    def verify(self, app_failed: bool = False):
        self._generated_files.verify(app_failed=app_failed)

    def save_app_logs_from_cluster(self, app_id):
        LOG.info("Saving application logs from cluster...")
        if app_id == APP_ID_NOT_AVAILABLE:
            return

        try:
            cmds = self.cluster.compress_and_download_app_logs(NODEMANAGER_SELECTOR, app_id,
                                                               workdir=self.current_tc_dir,
                                                               compress_dir=True)
            files = []
            for cmd in cmds:
                cmd.run()
                files.append(cmd.dest)
            self._generated_files.register_files(OutputFileType.APP_LOG_TAR_GZ, files)
        except HadesException as he:
            LOG.exception("Error while creating targz files of application logs!")
            if "Failed to compress app logs" in str(he):
                self.save_yarn_daemon_logs()
            else:
                raise he

        self.write_node_health_reports(self.cluster.get_state_and_health_report())

    def save_yarn_daemon_logs(self):
        nm_cmds = self.cluster.compress_and_download_daemon_logs(NODEMANAGER_SELECTOR,
                                                                 workdir=self.current_tc_dir)
        rm_cmds = self.cluster.compress_and_download_daemon_logs(RESOURCEMANAGER_SELECTOR,
                                                                 workdir=self.current_tc_dir)
        cmds = nm_cmds + rm_cmds
        files = []
        for cmd in cmds:
            cmd.run()
            files.append(cmd.dest)
        self._generated_files.register_files(OutputFileType.YARN_DAEMON_LOGS_TAR_GZ, files)

    def write_yarn_app_logs(self, app_name, app_command, app_log_lines):
        app_log_file = APP_LOG_FILE_NAME_FORMAT.format(app=app_name)
        app_log_file_path = os.path.join(self.current_tc_dir, app_log_file)

        LOG.debug("Writing app log file '%s' on host '%s'", app_log_file_path, app_command.target.host)
        with open(app_log_file_path, 'w') as f:
            f.writelines(app_log_lines)
        self._generated_files.register_files(OutputFileType.APP_LOG_FILE, [app_log_file])

    def decompress_app_container_logs(self):
        self._decompress_logs(OutputFileType.APP_LOG_TAR_GZ, OutputFileType.EXTRACTED_APP_LOG_FILES)

    def decompress_daemon_logs(self):
        self._decompress_logs(OutputFileType.YARN_DAEMON_LOGS_TAR_GZ, OutputFileType.EXTRACTED_YARN_DAEMON_LOG_FILES)

    def _decompress_logs(self, src_type: 'OutputFileType', dst_type: 'OutputFileType'):
        for tar_file in self._generated_files.get(src_type):
            target_dir = os.path.join(self.current_tc_dir)
            LOG.debug("[%s] Extracting file '%s' to %s", self.context, tar_file, target_dir)
            CompressedFileUtils.extract_targz_file(tar_file, target_dir)
            self._generated_files.register_files(dst_type,
                                                 CompressedFileUtils.list_targz_file(tar_file),
                                                 allow_multiple=True)

    def write_patch_file(self, patch_file):
        target_file = os.path.join(self.current_ctx_dir, os.path.basename(patch_file))
        patch_file = os.path.expanduser(patch_file)
        shutil.copyfile(patch_file, target_file)
        self._generated_files.register_files(OutputFileType.PATCH_FILE, [target_file])


@dataclass
class LogsByRoles:
    output_file_writer: 'OutputFileWriter'
    cluster: HadoopCluster
    selector: str
    roles = None
    log_commands = None

    def __post_init__(self):
        self.log_lines_dict = {}

    # TODO Move this to ClusterHandler? 
    def read_logs_into_dict(self):
        LOG.debug("Reading YARN logs from cluster...")

        self.roles = self.cluster.select_roles(self.selector)
        self.log_commands: List[RunnableCommand] = self.cluster.read_logs(follow=True, selector=self.selector)
        LOG.debug("YARN log commands: %s", self.log_commands)

        for read_logs_command in self.log_commands:
            LOG.debug("Running command '%s' in async mode on host '%s'", read_logs_command.cmd,
                      read_logs_command.target.host)
            read_logs_command.run_async(stdout=_callback(read_logs_command, self.log_lines_dict),
                                        stderr=_callback(read_logs_command, self.log_lines_dict))

    def search_in_logs(self, verification: LogVerification):
        LOG.info("Searching in logs, verification: %s", verification)
        filtered_roles = list(filter(lambda rc: rc.target.role_type in [verification.role_type], list(self.log_lines_dict.keys())))

        valid = True if verification.inverted_mode else False
        bad_role = None
        for role in filtered_roles:
            for line in self.log_lines_dict[role]:
                if verification.text in line:
                    if verification.inverted_mode:
                        LOG.info("Verification became invalid. Text: %s, Line: %s, Role: %s, Verification object: %s",
                                  verification.text, line, role, verification)
                        valid = False
                        bad_role = role
                        break
                    else:
                        LOG.info("Verification became valid. Text: %s, Line: %s, Role: %s, Verification object: %s",
                                  verification.text, line, role, verification)
                        valid = True
                        break

        if not valid:
            if verification.inverted_mode:
                raise HadesException(
                    "Found log line in NM log: '{}'.\n"
                    "This shouldn't have been included in the current version's logs.\n"
                    "NM log that include the line: {}\n"
                    "Lines: {}".format(bad_role, verification.text, self.log_lines_dict[bad_role]))
            else:
                raise HadesException(
                    "Not found log line in none of the NM logs: '{}'.\n"
                    "This should have been included in the current version's logs.\n"
                    "All lines for NM logs: {}".format(verification.text, self.log_lines_dict))

    def verify_no_empty_lines(self, raise_exc=True):
        empty_lines_per_role = []
        cmd_by_role = {cmd.target: cmd for cmd in self.log_commands}
        for r in self.roles:
            cmd = cmd_by_role[r]
            lines = self.log_lines_dict[cmd]
            if not lines:
                empty_lines_per_role.append(r)
        # LOG.debug("***cmd_by_role: %s", cmd_by_role)
        if empty_lines_per_role and raise_exc:
            raise HadesException(f"Found empty lines for the following roles: {empty_lines_per_role}")


@dataclass
class Netty4TestContext:
    name: str
    base_branch: str = DEFAULT_BRANCH
    patch_file: str = None
    log_verifications: List[LogVerification] = field(default_factory=list)
    invert_nm_log_msg_verification: str = None
    compile: bool = True
    allow_verification_failure: bool = False

    def __str__(self):
        return f"Context: {self.name}"

    def __hash__(self):
        return hash(self.name)

    @property
    def get_dirname(self):
        repl = self.name.replace(" ", "_")
        return f"ctx_{repl}"


@dataclass
class Netty4TestConfig:
    only_run_testcase = "shuffle_ssl_enabled"  # TODO
    LIMIT_TESTCASES = False
    QUICK_MODE = False
    testcase_limit = 1 if QUICK_MODE or LIMIT_TESTCASES else TC_LIMIT_UNLIMITED
    enable_compilation = False if QUICK_MODE else True
    allow_verification_failure = True if QUICK_MODE else False

    extract_tar_files = True
    timeout = 120
    compress_tc_result = False
    decompress_app_container_logs = True
    decompress_daemon_logs = True
    shufflehandler_log_level = HadoopLogLevel.DEBUG
    cache_built_maven_artifacts = True
    halt_execution_on_failed_job = True
    halt_execution_on_job_timeout = True  # TODO
    loadgen_no_mappers = 4
    loadgen_no_reducers = 3
    loadgen_timeout = 1000
    # TODO add switch that simulates an intentional job failure?

    force_compile = False

    def __post_init__(self):
        sleep_job = MapReduceApp(MapReduceAppType.SLEEP, cmd='sleep -m 1 -r 1 -mt 10 -rt 10', timeout=self.timeout)
        pi_job = MapReduceApp(MapReduceAppType.PI, cmd='pi 1 1000', timeout=self.timeout)
        loadgen_job = MapReduceApp(MapReduceAppType.LOADGEN,
                                   cmd=f"loadgen -m {self.loadgen_no_mappers} -r {self.loadgen_no_reducers} "
                                       f"-outKey org.apache.hadoop.io.Text "
                                       f"-outValue org.apache.hadoop.io.Text",
                                   timeout=self.loadgen_timeout)

        sort_input_dir = "/user/systest/sortInputDir"
        sort_output_dir = "/user/systest/sortOutputDir"
        random_writer_job = MapReduceApp(MapReduceAppType.RANDOM_WRITER, cmd=f"randomwriter {sort_input_dir}")
        mapred_sort_job = MapReduceApp(MapReduceAppType.TEST_MAPRED_SORT,
                                       cmd=f"testmapredsort "
                                           f"-sortInput {sort_input_dir} "
                                           f"-sortOutput {sort_output_dir}")

        self.mr_apps: Dict[MapReduceAppType, MapReduceApp] = {
            MapReduceAppType.SLEEP: sleep_job,
            MapReduceAppType.PI: pi_job,
            MapReduceAppType.TEST_MAPRED_SORT: mapred_sort_job,
            MapReduceAppType.RANDOM_WRITER: random_writer_job,
            MapReduceAppType.LOADGEN: loadgen_job
        }
        self.default_apps = [MapReduceAppType.SLEEP, MapReduceAppType.LOADGEN]

        patch = "~/googledrive/development_drive/_upstream/HADOOP-15327/patches/backup-patch-test-changes-20220815.patch"
        netty_log_message = "*** HADOOP-15327: netty upgrade"
        self.contexts = [Netty4TestContext("without netty patch on trunk",
                                           DEFAULT_BRANCH,
                                           log_verifications=[LogVerification(HadoopRoleType.NM, netty_log_message, inverted_mode=True)],
                                           compile=self.enable_compilation or self.force_compile,
                                           allow_verification_failure=self.allow_verification_failure),
                         Netty4TestContext("with netty patch based on trunk",
                                           DEFAULT_BRANCH,
                                           patch_file=patch,
                                           log_verifications=[LogVerification(HadoopRoleType.NM, netty_log_message, inverted_mode=False)],
                                           compile=self.enable_compilation or self.force_compile,
                                           allow_verification_failure=self.allow_verification_failure
                                           )]

        self.testcases = [
            *Netty4TestcasesBuilder("shuffle_max_connections")
            .with_configs(SHUFFLE_MAX_CONNECTIONS, ["5", "10"])
            .with_apps(self.default_apps)
            .generate_testcases(self),
            *Netty4TestcasesBuilder("shuffle_max_threads")
            .with_configs(SHUFFLE_MAX_THREADS, ["3", "6"])
            .with_apps(self.default_apps)
            .generate_testcases(self),
            *Netty4TestcasesBuilder("shuffle_max_open_files")
            .with_configs(SHUFFLE_MAX_SESSION_OPEN_FILES, ["2", "5"])
            .with_apps(self.default_apps)
            .generate_testcases(self),
            *Netty4TestcasesBuilder("shuffle_listen_queue_size")
            .with_configs(SHUFFLE_LISTEN_QUEUE_SIZE, ["10", "50"])
            .with_apps(self.default_apps)
            .generate_testcases(self),
            *Netty4TestcasesBuilder("shuffle_ssl_enabled")
            .with_configs(SHUFFLE_SSL_ENABLED, ["true"])
            .with_apps(self.default_apps)
            .generate_testcases(self),
            *Netty4TestcasesBuilder("keepalive")
            .with_config(SHUFFLE_CONNECTION_KEEPALIVE_ENABLE, "true")
            .with_configs(SHUFFLE_CONNECTION_KEEPALIVE_TIMEOUT, ["15", "25"])
            .with_apps(self.default_apps)
            .generate_testcases(self)
        ]

        if self.only_run_testcase:
            tmp = []
            for tc in self.testcases:
                if tc.simple_name == self.only_run_testcase:
                    tmp.append(tc)
            if not tmp:
                raise HadesException("Cannot find any testcase matching name '{}'".format(self.only_run_testcase))

            self.testcases = tmp
            LOG.info("Found testcases matching for name '%s': %s", self.only_run_testcase, self.testcases)


class Netty4TestResults:
    def __init__(self):
        self.results: Dict[Netty4TestContext, Dict[Netty4Testcase, TestcaseResult]] = {}
        self.context = None
        self.tc = None

    @property
    def current_result(self) -> TestcaseResult:
        return self.results[self.context][self.tc]

    @property
    def is_current_tc_timed_out(self):
        if not self.context or not self.tc:
            return False
        curr = self.results[self.context][self.tc]
        return curr.type == TestcaseResultType.TIMEOUT

    @property
    def is_current_tc_failed(self):
        if not self.context or not self.tc:
            return False
        curr = self.results[self.context][self.tc]
        return curr.type == TestcaseResultType.FAILED

    def update_with_context_and_testcase(self, context, testcase):
        self.context = context
        self.results[self.context] = {}
        self.tc = testcase

    def update_with_result(self, tc, result):
        self.results[self.context][tc] = result

    def print_report(self):
        sep = "=" * 60
        LOG.info(sep)
        LOG.info("TESTCASE RESULTS")
        LOG.info(sep)

        data = []
        for tc, result in self.results[self.context].items():
            data.append([tc.name, result.app_command.cmd, result.type.value, result.details])
        tabulated = tabulate(data, ["TESTCASE", "COMMAND", "RESULT", "DETAILS"], tablefmt="fancy_grid")
        LOG.info("\n" + tabulated)

    def compare(self, reference_ctx):
        LOG.debug("PRINTING ALL RESULTS: %s", self.results)
        results1 = self.results[reference_ctx]

        for context, results in self.results.items():
            if results != results1:
                raise HadesException("Different results for contexts!\n"
                                     "Context1: {}, results: {}\n"
                                     "Context2: {}, results: {}".format(reference_ctx, results1, context, results))


class OutputFileType(Enum):
    INITIAL_CONFIG_MR = "initial_config_mr"
    INITIAL_CONFIG_YARN_SITE = "initial_config_yarn_site"
    INITIAL_CONFIG_CORE_SITE = "initial_config_core_site"
    INITIAL_CONFIG_SSL_SERVER = "initial_config_ssl_server"
    INITIAL_CONFIG_LOG4J_PROPERTIES = "initial_config_log4j_properties"

    TC_CONFIG_MR = "testcase_config_mr"
    TC_CONFIG_YARN_SITE = "testcase_config_yarn_site"
    TC_CONFIG_CORE_SITE = "testcase_config_core_site"
    TC_CONFIG_SSL_SERVER = "testcase_config_ssl_server"
    TC_CONFIG_LOG4J_PROPERTIES = "testcase_config_log4j_properties"

    APP_LOG_TAR_GZ = "app_log_tar_gz"
    YARN_DAEMON_LOGS_TAR_GZ = "yarn_daemon_logs_tar_gz"
    ALL_FILES_TAR_GZ = "all_files_tar_gz"
    NM_RESTART_LOGS = "nm_restart_logs"
    YARN_DAEMON_LOGS = "yarn_daemon_logs"
    APP_LOG_FILE = "app_log_file"
    EXTRACTED_APP_LOG_FILES = "extracted_app_log_files"
    EXTRACTED_YARN_DAEMON_LOG_FILES = "extracted_yarn_daemon_log_files"
    PATCH_FILE = "patch_file"
    NODE_HEALTH_REPORTS = "node_health_reports"


class GeneratedOutputFiles:
    def __init__(self):
        self._files: Dict[Netty4TestContext, Dict[Netty4Testcase, Dict[OutputFileType, List[str]]]] = {}
        self.ctx = None
        self.tc = None
        self._curr_files_dict = None

    def update_with_ctx_and_tc(self, context, tc):
        self.ctx = context
        self.tc = tc

        self._files[self.ctx] = {}
        self._files[self.ctx][self.tc] = {}
        self._curr_files_dict = self._files[self.ctx][self.tc]

    def register_files(self, out_type: OutputFileType, files: List[str], allow_multiple: bool = False):
        if out_type in self._curr_files_dict and not allow_multiple:
            raise HadesException("Output type is already used: {}".format(out_type))
        self._curr_files_dict[out_type] = files

    def get(self, out_type):
        if out_type not in self._curr_files_dict:
            return []
        return self._curr_files_dict[out_type]

    def get_all_for_current_ctx(self):
        return list(itertools.chain.from_iterable(self._curr_files_dict.values()))

    def print_all(self):
        LOG.debug("All generated files: %s", self._files)

    def print_all_for_current_ctx(self):
        LOG.debug("All files for context '%s' / testcase '%s': %s", self.ctx, self.tc, self._curr_files_dict)

    def verify(self, app_failed: bool = False):
        LOG.info("Verifying generated output files...")
        if not self.get(OutputFileType.TC_CONFIG_MR):
            raise HadesException("Expected non-empty testcase config mapred-site.xml files list!")
        if not self.get(OutputFileType.TC_CONFIG_YARN_SITE):
            raise HadesException("Expected non-empty testcase config yarn-site.xml files list!")
        if not self.get(OutputFileType.INITIAL_CONFIG_MR):
            raise HadesException("Expected non-empty initial config files list!")
        if not self.get(OutputFileType.YARN_DAEMON_LOGS):
            raise HadesException("Expected non-empty YARN log files list!")
        if not self.get(OutputFileType.NM_RESTART_LOGS):
            raise HadesException("Expected non-empty YARN NM restart log files!")

        if app_failed and not self.get(OutputFileType.YARN_DAEMON_LOGS_TAR_GZ):
            raise HadesException("App failed. Expected non-empty daemon logs for YARN processes!")

        if not app_failed and (not self.get(OutputFileType.YARN_DAEMON_LOGS_TAR_GZ) or not self.get(OutputFileType.APP_LOG_TAR_GZ)):
            raise HadesException("App not failed. Expected non-empty daemon logs for YARN processes and app log tar files list!")


@dataclass
class BuildContext:
    patch_file_path: str
    cache_key: str
    built_jars: Dict[str, str]


class Compiler:
    def __init__(self, workdir, context, handler, config):
        self.workdir = workdir
        self.context = context
        self.handler = handler
        self.config = config
        self.build_contexts = {}
        self.db_file = os.path.join(self.workdir, "db", "db.pickle")

    def compile(self):
        LOG.info("Compile set to %s, force compile: %s", self.context.compile, self.config.force_compile)
        if self.context.compile:
            patch_file = self.context.patch_file
            hadoop_dir = HadoopDir(self.handler.ctx.config.hadoop_path)
            branch = hadoop_dir.get_current_branch(fallback="trunk")

            compilation_required = True
            if not self.config.force_compile and self.config.cache_built_maven_artifacts:
                self.build_contexts = self.load_db()
                hadoop_dir.extract_changed_modules(allow_empty=True)
                changed_jars = hadoop_dir.get_changed_jar_paths()
                all_loaded, cached_modules = self.load_from_cache(branch, patch_file, changed_jars)
                if all_loaded:
                    compilation_required = False
                    LOG.info("Found all required jars in the jar cache! Jars: %s", cached_modules)

            if compilation_required:
                LOG.info("[%s] Starting compilation...", self.context)
                changed_modules: Dict[str, str] = self.handler.compile(all=True, changed=False, deploy=True, modules=None, no_copy=True, single=None)
                self.save_to_cache(branch, patch_file, changed_modules)

    @staticmethod
    def _make_key(branch, patch_file):
        if not patch_file:
            patch_file = "without_patch"
        return f"{branch}_{patch_file}"

    def _build_cache_path_for_jars(self, branch, patch_file, changed_jars):
        cache_paths = {}
        for module_name, module_path in changed_jars.items():
            jar_path = module_path.replace(self.handler.ctx.config.hadoop_path, "").lstrip(os.sep)
            cache_paths[module_name] = os.path.join(self.workdir, "jarcache", self._make_key(branch, patch_file), jar_path)
        return cache_paths

    def load_from_cache(self, branch, patch_file, changed_jars):
        cached_modules = self._build_cache_path_for_jars(branch, patch_file, changed_jars)
        for module_name, module_path in cached_modules.items():
            if not os.path.exists(module_path):
                LOG.info("Module '%s' not found in cache at location: %s", module_name, module_path)
                return False, cached_modules
            LOG.debug("Module '%s' found in cache at location: %s", module_name, module_path)
        return True, cached_modules

    def save_to_cache(self, branch, patch_file, changed_modules):
        cached_modules = self._build_cache_path_for_jars(branch, patch_file, changed_modules)
        if self.config.cache_built_maven_artifacts:
            modules_in_cache = {}
            cache_key = self._make_key(branch, patch_file)
            for module, module_path in cached_modules.items():
                modules_in_cache[module] = module_path
                FileUtils.ensure_dir_created(os.path.dirname(module_path))
                shutil.copyfile(changed_modules[module], module_path)
            build_context = BuildContext(patch_file, cache_key, modules_in_cache)
            self.build_contexts[cache_key] = build_context
            self.write_db()

    def load_db(self):
        LOG.debug("Loading DB from file: %s", self.db_file)
        if os.path.exists(self.db_file):
            with open(self.db_file, "rb") as db:
                return pickle.load(db)
        return {}

    def write_db(self):
        LOG.debug("Writing DB to file: %s", self.db_file)
        FileUtils.ensure_dir_created(os.path.dirname(self.db_file))
        with open(self.db_file, "wb") as db:
            pickle.dump(self.build_contexts, db)


class ExecutionState(Enum):
    RUNNING = "running"
    HALTED = "halted"


class Netty4RegressionTestSteps:
    def __init__(self, test, no_of_testcases, handler):
        self.overwrite_config_func = test.overwrite_config
        self.no_of_testcases = no_of_testcases
        self.handler = handler
        self.cluster_handler = ClusterHandler(test.cluster)
        self.cluster_config_updater = ClusterConfigUpdater(test.cluster, test.session_dir)
        self.cluster = test.cluster
        self.config = test.config
        self.workdir = test.workdir
        self.session_dir = test.session_dir
        self.using_custom_workdir = test.using_custom_workdir
        self.test_results = Netty4TestResults()
        self.context: Netty4TestContext = None
        self.app_id = None
        self.hadoop_config = None
        self.nm_restart_logs = None
        self.yarn_logs = None
        self.output_file_writer = None
        self.tc = None
        self.compiler = None
        self.execution_state = ExecutionState.RUNNING

    def start_context(self, context):
        if self._should_halt():
            LOG.info("Execution halted as last job failed!")
            self.execution_state = ExecutionState.HALTED
            return self.execution_state

        self.context = context
        LOG.info("Starting: %s", self.context)
        PrintUtils.print_banner_figlet(f"STARTING CONTEXT: {self.context}")
        self.output_file_writer = OutputFileWriter(self.cluster)
        self.compiler = Compiler(self.workdir, self.context, self.handler, self.config)
        return ExecutionState.RUNNING

    def _should_halt(self):
        return self.test_results.is_current_tc_failed and self.config.halt_execution_on_failed_job or \
               (self.test_results.is_current_tc_timed_out and self.config.halt_execution_on_job_timeout)

    def setup_branch_and_patch(self):
        # Checkout branch, apply patch if required
        hadoop_dir = HadoopDir(self.handler.ctx.config.hadoop_path)
        hadoop_dir.switch_branch_to(self.context.base_branch)

        if self.context.patch_file:
            hadoop_dir.apply_patch(self.context.patch_file, force_reset=True)

    def load_default_yarn_site_configs(self):
        LOG.info("Loading default yarn-site.xml configs for NodeManagers...")
        self.cluster_config_updater.load_configs(HadoopConfigFile.YARN_SITE,
                                                 ClusterConfigUpdater.YARN_SITE_DEFAULT_CONFIGS,
                                                 NODEMANAGER_SELECTOR)

    def load_default_mapred_configs(self):
        LOG.info("Loading default MR ShuffleHandler configs for NodeManagers...")
        self.cluster_config_updater.load_configs(HadoopConfigFile.MAPRED_SITE,
                                                 DEFAULT_CONFIGS,
                                                 NODEMANAGER_SELECTOR)

    def load_default_core_site_configs(self):
        LOG.info("Loading default core-site.xml configs for NodeManagers...")
        self.cluster_config_updater.load_configs(HadoopConfigFile.CORE_SITE,
                                                 DEFAULT_CORE_SITE_CONFIGS,
                                                 NODEMANAGER_SELECTOR)

    def load_default_ssl_server_configs(self):
        LOG.info("Loading default ssl-server.xml configs for NodeManagers...")
        configs = DEFAULT_SSL_SERVER_CONFIGS
        configs["ssl.server.keystore.location"] = self.keystore_file_location
        self.cluster_config_updater.load_configs(HadoopConfigFile.SSL_SERVER,
                                                 configs,
                                                 NODEMANAGER_SELECTOR,
                                                 allow_empty=True)

    def init_testcase(self, tc):
        if self._should_halt():
            self.execution_state = ExecutionState.HALTED
            return self.execution_state

        self.tc = tc
        self.test_results.update_with_context_and_testcase(self.context, self.tc)
        self.output_file_writer.update_with_context_and_testcase(self.session_dir, self.context, self.tc)
        LOG.info("[%s] [%d / %d] Running testcase: %s", self.context, self.output_file_writer.tc_no, self.no_of_testcases,
                 self.tc)
        PrintUtils.print_banner_figlet(f"STARTING TESTCASE: {self.tc.name}")

        if self.context.patch_file:
            self.output_file_writer.write_patch_file(self.context.patch_file)
        return ExecutionState.RUNNING

    def load_default_configs(self):
        LOG.info("Loading default configs...")
        self.load_default_mapred_configs()
        self.load_default_core_site_configs()
        self.load_default_ssl_server_configs()
        self.hadoop_config = HadoopConfig(HadoopConfigFile.MAPRED_SITE)
        self.output_file_writer.write_initial_config_files()

    def apply_testcase_configs(self):
        LOG.info("Applying testcase configs...")
        for config_key, config_val in self.tc.config_changes.items():
            self.hadoop_config.extend_with_args({config_key: config_val})

        self.cluster.update_config(NODEMANAGER_SELECTOR, self.hadoop_config, no_backup=True, workdir=self.session_dir)

    def set_shufflehandler_loglevel(self):
        LOG.debug("Setting ShuffleHandler log level to: %s", self.config.shufflehandler_log_level)

        set_log_level_cmds: List[RunnableCommand] = self.cluster.set_log_level(
            selector=YARN_SELECTOR,
            package="org.apache.hadoop.mapred.ShuffleHandler",
            log_level=self.config.shufflehandler_log_level)
        LOG.debug("YARN set log level commands: %s", set_log_level_cmds)
        if not set_log_level_cmds:
            raise HadesException("No 'set ShuffleHandler loglevel' commands were created!")

        for cmd in set_log_level_cmds:
            LOG.debug("Running command '%s' in async mode on host '%s'", cmd.cmd, cmd.target.host)
            cmd.run()

    def restart_nms_and_save_logs(self):
        LOG.info("Restarting NodeManagers with new configs...")
        self.nm_restart_logs = LogsByRoles(self.output_file_writer, self.cluster, selector=NODEMANAGER_SELECTOR)
        self.cluster_handler.restart_nms(logs_by_roles=self.nm_restart_logs)
        self.output_file_writer.write_nm_restart_logs(self.nm_restart_logs)

    def run_app_and_collect_logs_to_file(self, app: MapReduceApp):
        app_log_lines = []
        result_type = TestcaseResultType.PASSED
        with self.overwrite_config_func(cmd_prefix="sudo -u systest"):
            app_command = self.cluster.run_app(app, selector=NODEMANAGER_SELECTOR)

        LOG.debug("Running app command '%s' in async mode on host '%s'", app_command.cmd, app_command.target.host)
        try:
            app_command.run_async(block=True, stderr=lambda line: app_log_lines.append(line), timeout=app.get_timeout_seconds())
        except HadesCommandTimedOutException:
            LOG.exception("Failed to run app command '%s'. Command '%s' timed out after %d seconds",
                          app_command.cmd, app_command, app.get_timeout_seconds())
            result_type = TestcaseResultType.TIMEOUT
        except CommandExecutionException as e:
            LOG.exception("Failed to run app command '%s'. Command '%s' failed.", app_command.cmd, app_command)
            result_type = TestcaseResultType.FAILED

        self.output_file_writer.write_yarn_app_logs(app.name, app_command, app_log_lines)

        if result_type == TestcaseResultType.TIMEOUT:
            details_msg = TIMEOUT_MSG_TEMPLATE.format(app.get_timeout_seconds())
        elif result_type == TestcaseResultType.FAILED:
            details_msg = ERROR_MSG_TEMPLATE.format(app.get_timeout_seconds())
        else:
            details_msg = ""

        result = TestcaseResult(result_type, app_command, details=details_msg)
        self.test_results.update_with_result(self.tc, result)

    def start_to_collect_yarn_daemon_logs(self):
        LOG.info("Starting to collect live YARN daemon logs...")
        self.yarn_logs = LogsByRoles(self.output_file_writer, self.cluster, selector=YARN_SELECTOR)
        self.yarn_logs.read_logs_into_dict()

    def get_application(self):
        if self.test_results.is_current_tc_timed_out:
            LOG.debug("[%s] Getting running app id as testcase timed out", self.context)
            self.app_id = self.cluster_handler.get_latest_running_app()
        elif self.test_results.is_current_tc_failed:
            LOG.debug("[%s] Not getting any apps as testcase failed", self.context)
        else:
            LOG.debug("[%s] Getting finished app id as testcase passed", self.context)
            self.app_id = self.cluster_handler.get_latest_finished_app()

    def ensure_if_hadoop_version_is_correct(self):
        LOG.info("Ensuring if cluster's Hadoop version is correct...")
        if self.context.log_verifications:
            try:
                for verification in self.context.log_verifications:
                    self.yarn_logs.search_in_logs(verification)
            except HadesException as e:
                if self.context.allow_verification_failure:
                    LOG.exception("Verification failed: %s, but allow verification failure is set to True!", verification)
                else:
                    raise e
        else:
            LOG.warning("No verifications are defined while checking cluster's Hadoop version")

    def write_result_files(self):
        LOG.info("Writing result files...")
        self.output_file_writer.write_yarn_logs(self.yarn_logs)

        app_failed = self.test_results.is_current_tc_failed
        if not app_failed:
            self.output_file_writer.save_app_logs_from_cluster(self.app_id)
        else:
            LOG.warning("Not saving app logs as last app failed, but saving YARN daemon logs!")
        self.output_file_writer.save_yarn_daemon_logs()
        self.output_file_writer.write_testcase_config_files()
        self.output_file_writer.verify(app_failed=app_failed)

    def verify_daemon_logs(self):
        self.yarn_logs.verify_no_empty_lines()
        self.nm_restart_logs.verify_no_empty_lines()

    def finalize_testcase_data_files(self):
        if self.config.decompress_app_container_logs:
            self.output_file_writer.decompress_app_container_logs()

        if self.config.decompress_daemon_logs:
            self.output_file_writer.decompress_daemon_logs()

        if self.config.compress_tc_result:
            self.output_file_writer.create_compressed_testcase_data_file(using_custom_workdir=self.using_custom_workdir)

    def finalize_testcase(self):
        self.output_file_writer.print_all_generated_files_for_current_ctx()

    def finalize_context(self):
        self.output_file_writer.print_all_generated_files()
        self.test_results.print_report()

    def compare_results(self):
        if self.config.testcase_limit < TC_LIMIT_UNLIMITED:
            LOG.warning("Skipping comparison of testcase results as the testcase limit is set to: %s", self.config.testcase_limit)
            return
        self.test_results.compare(self.config.contexts[0])

    def compile(self):
        self.compiler.compile()

    def verify_nm_configs(self, dict: Dict[HadoopConfigFile, List[Tuple[str, str]]]):
        self.cluster_handler.verify_nm_configs(dict)

    def restart_services(self):
        handlers = []

        cmds = self.cluster.restart_roles(YARN_SELECTOR)
        for cmd in cmds:
            handlers.append(cmd.run_async())

        for h in handlers:
            h.wait()

        # TODO Ensure that services are actually running!


class ClusterConfigUpdater:
    YARN_SITE_DEFAULT_CONFIGS = {
        CONF_DEBUG_DELAY: str(UNLIMITED),
        # Other YARN configs

        # To avoid unhealthy node issues
        # Example:
        # URL: http://ccycloud-1.snemeth-netty.root.hwx.site:8088/cluster/nodes/unhealthy
        # 1/1 local-dirs usable space is below configured utilization percentage/no more usable space
        # [ /tmp/hadoop-systest/nm-local-dir : used space above threshold of 90.0% ] ;
        # 1/1 log-dirs usable space is below configured utilization percentage/no more usable space
        # [ /tmp/hadoop-logs : used space above threshold of 90.0% ]
        CONF_DISK_MAX_UTILIZATION: CONF_DISK_MAX_UTILIZATION_VAL
    }
    
    def __init__(self, cluster, workdir):
        self.cluster = cluster
        self.workdir = workdir
    
    def load_configs(self, conf_file_type, conf_dict, selector, allow_empty: bool = False):
        default_config = HadoopConfig(conf_file_type)
        for k, v in conf_dict.items():
            if isinstance(v, int):
                v = str(v)
            default_config.extend_with_args({k: v})
        self.cluster.update_config(selector, default_config, no_backup=True, workdir=self.workdir, allow_empty=allow_empty)

    def load_properties_configs(self, conf_file_type, conf_dict, selector, allow_empty: bool = False):
        allowed_config_file_types = [HadoopConfigFile.LOG4J_PROPERTIES]
        if conf_file_type not in allowed_config_file_types:
            raise HadesException("Config file type '{}' is not in allowed types: {}".format(conf_file_type, allowed_config_file_types))

        default_config = HadoopPropertiesConfig(conf_file_type)
        for k, v in conf_dict.items():
            if isinstance(v, int):
                v = str(v)
            default_config.extend_with_args({k: v})
        # TODO
        self.cluster.update_config(selector, default_config, no_backup=True, workdir=self.workdir, allow_empty=allow_empty)

        
class ClusterHandler:
    def __init__(self, cluster):
        self.cluster = cluster
    
    def restart_nms(self, logs_by_roles: LogsByRoles):
        logs_by_roles.read_logs_into_dict()
        handlers = []
        # TODO Move this restart logic to Cluster or Executor?
        role_pids_before = self.cluster.get_role_pids(NODEMANAGER_SELECTOR)
        for cmd in self.cluster.restart_roles(NODEMANAGER_SELECTOR):
            handlers.append(cmd.run_async())
        for h in handlers:
            h.wait()

        # Under some circumstances, Nodemanager not always stopped when command ran: 'yarn --daemon stop nodemanager'
        # Verify that pids of NM processes are different after restart
        role_pids_after = self.cluster.get_role_pids(NODEMANAGER_SELECTOR)

        same_pids = self._verify_if_pids_are_different(role_pids_after, role_pids_before)

        if same_pids:
            LOG.warning("pids of NodeManagers are the same before and after restart: %s. Trying to kill the processes and start NodeManagers.", same_pids)
        self.cluster.force_restart_roles(NODEMANAGER_SELECTOR)

        # Check pids again
        role_pids_after = self.cluster.get_role_pids(NODEMANAGER_SELECTOR)
        same_pids = self._verify_if_pids_are_different(role_pids_after, role_pids_before)
        if same_pids:
            raise HadesException("pids of NodeManagers are the same before and after restart (even after tried to kill them manually): {}".format(same_pids))

    @staticmethod
    def _verify_if_pids_are_different(role_pids_after, role_pids_before):
        nm_hostnames = role_pids_before.keys()
        same_pids = {}
        for host in nm_hostnames:
            if role_pids_before[host] == role_pids_after[host]:
                same_pids[host] = role_pids_after[host]
        return same_pids

    def get_single_running_app(self):
        cmd = self.cluster.get_running_apps()
        running_apps, stderr = cmd.run()
        if len(running_apps) > 1:
            raise ScriptException(f"Expected 1 running application. Found more: {running_apps}")
        elif len(running_apps) == 0:
            raise ScriptException("Expected 1 running application. Found no application")
        current_app_id = running_apps[0]
        LOG.info("Found running application: %s", current_app_id)
        return current_app_id

    def get_latest_running_app(self):
        cmd = self.cluster.get_running_apps()
        running_apps, stderr = cmd.run()
        if len(running_apps) == 0:
            raise ScriptException("Expected 1 running application. Found no application")
        current_app_id = running_apps[0]
        LOG.info("Found running application: %s", current_app_id)
        return current_app_id

    def get_latest_finished_app(self):
        cmd = self.cluster.get_finished_apps()
        finished_apps, stderr = cmd.run()
        LOG.info("Found finished applications: %s", finished_apps)
        # Topmost row is the latest app
        return finished_apps[0]

    def verify_nm_configs(self, dict: Dict[HadoopConfigFile, List[Tuple[str, str]]]):
        configs_per_nm = self.cluster.get_config_from_api(NODEMANAGER_SELECTOR)
        for config_file, expected_conf_list in dict.items():
            # We are not interested in the file types in this case (whether it's yarn-site.xml / mapred-site.xml or something else)
            for expected_conf_tup in expected_conf_list:
                for host, nm_conf in configs_per_nm.items():
                    conf_name = expected_conf_tup[0]
                    exp_value = expected_conf_tup[1]

                    not_found = conf_name not in nm_conf
                    values_differ = nm_conf[conf_name] != exp_value
                    if not_found or values_differ:
                        act_val = "XXX"
                        if not_found:
                            act_val = "not found"
                        elif values_differ:
                            act_val = nm_conf[conf_name]
                        raise HadesException("Invalid config value found for NM on host '{}'. "
                                             "Config name: {}, Expected value: {}, Actual value: {}"
                                             .format(host, conf_name, exp_value, act_val))


class Netty4RegressionTestDriver(HadesScriptBase):
    def __init__(self, cluster: HadoopCluster, workdir: str, session_dir: str):
        super().__init__(cluster, workdir, session_dir)
        self.config = Netty4TestConfig()
        self.context = None
        self.output_file_writer = None

    def run(self, handler: MainCommandHandler):
        testcases = self.config.testcases
        LOG.info("ALL Testcases: %s", testcases)
        if self.config.testcase_limit > 0:
            LOG.info("Limiting testcases to %s", self.config.testcase_limit)
            testcases = testcases[:self.config.testcase_limit]

        self._run_testcases(testcases, handler)

    def _run_testcases(self, testcases, handler):
        no_of_testcases = len(testcases)
        LOG.info("Will run %d testcases", no_of_testcases)
        self.steps = Netty4RegressionTestSteps(self, no_of_testcases, handler)
        for context in self.config.contexts:
            exec_state = self.steps.start_context(context)
            if exec_state == ExecutionState.HALTED:
                LOG.warning("Stopping test driver execution as execution state is HALTED!")
                break
            self.steps.setup_branch_and_patch()
            self.steps.compile()
            self.steps.load_default_yarn_site_configs()
            
            for idx, tc in enumerate(testcases):
                exec_state = self.steps.init_testcase(tc)  # 1. Update context, TestResults
                if exec_state == ExecutionState.HALTED:
                    LOG.warning("Stopping test driver execution as execution state is HALTED!")
                    break

                self.steps.restart_services()
                self.steps.load_default_configs()  # 2. Load default configs / Write initial config files                
                self.steps.apply_testcase_configs()  # 3. Apply testcase configs
                self.steps.set_shufflehandler_loglevel()  # 4. Set log level of ShuffleHandler to DEBUG
                self.steps.restart_nms_and_save_logs()  # 5. Restart NMs, Save logs
                self.steps.verify_nm_configs({
                    HadoopConfigFile.YARN_SITE: [(CONF_DISK_MAX_UTILIZATION, CONF_DISK_MAX_UTILIZATION_VAL)],
                })
                self.steps.start_to_collect_yarn_daemon_logs()  # 6. Start to collect YARN daemon logs
                self.steps.run_app_and_collect_logs_to_file(tc.app)  # 7. Run app of the testcase + Record test results
                self.steps.get_application()  # 8. Get application according to status
                self.steps.ensure_if_hadoop_version_is_correct()  # 9. Ensure if version is correct by checking certain logs
                self.steps.write_result_files()  # 10. Write YARN daemon logs - Now it's okay to write the YARN log files as the app is either finished or timed out
                self.steps.verify_daemon_logs()  # 11. Verify YARN daemon logs - Whether they are empty
                self.steps.finalize_testcase_data_files()  # 12. Write compressed / decompressed testcase data output files 
                self.steps.finalize_testcase()  # 13. Finalize testcase
            self.steps.finalize_context()
        self.steps.compare_results()
