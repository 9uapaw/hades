import itertools
import logging
import os.path
import pickle
import re
import shutil
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Callable, List, Dict, Tuple, Any

from tabulate import tabulate

from core.cmd import RunnableCommand
from core.error import ScriptException, HadesCommandTimedOutException, HadesException, CommandExecutionException
from core.handler import MainCommandHandler
from core.util import FileUtils, CompressedFileUtils, PrintUtils, StringUtils
from hadoop.app.example import MapReduceApp, MapReduceAppType
from hadoop.cluster import HadoopCluster, HadoopLogLevel
from hadoop.config import HadoopPropertiesConfig, HadoopConfigBase
from hadoop.hadoop_config import HadoopConfigFile
from hadoop.role import HadoopRoleType
from hadoop_dir.module import HadoopDir
from script.base import HadesScriptBase

INVALID_CONFIG_VALUE = "INVALID"

PACKAGE_SECURITY_SSL = "org.apache.hadoop.security.ssl"
PACKAGE_SHUFFLEHANDLER = "org.apache.hadoop.mapred.ShuffleHandler"

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


class ConfigWithDefault(Enum):
    SHUFFLE_MANAGE_OS_CACHE = (MAPREDUCE_SHUFFLE_PREFIX + ".manage.os.cache", "true")
    SHUFFLE_READAHEAD_BYTES = (MAPREDUCE_SHUFFLE_PREFIX + ".readahead.bytes", 4 * 1024 * 1024)
    SHUFFLE_MAX_CONNECTIONS = (MAPREDUCE_SHUFFLE_PREFIX + ".max.connections", 0)
    SHUFFLE_MAX_THREADS = (MAPREDUCE_SHUFFLE_PREFIX + ".max.threads", 0)
    SHUFFLE_TRANSFER_BUFFER_SIZE = (MAPREDUCE_SHUFFLE_PREFIX + ".transfer.buffer.size", 128 * 1024)
    SHUFFLE_TRANSFERTO_ALLOWED = (MAPREDUCE_SHUFFLE_PREFIX + ".transferTo.allowed", "true")
    SHUFFLE_MAX_SESSION_OPEN_FILES = (MAPREDUCE_SHUFFLE_PREFIX + ".max.session-open-files", 3)
    SHUFFLE_LISTEN_QUEUE_SIZE = (MAPREDUCE_SHUFFLE_PREFIX + ".listen.queue.size", 128)
    SHUFFLE_PORT = (MAPREDUCE_PREFIX + ".port", 13562)
    SHUFFLE_SSL_FILE_BUFFER_SIZE = (MAPREDUCE_SHUFFLE_PREFIX + ".ssl.file.buffer.size", 60 * 1024)
    SHUFFLE_CONNECTION_KEEPALIVE_ENABLE = (MAPREDUCE_SHUFFLE_PREFIX + ".connection-keep-alive.enable", "false")
    SHUFFLE_CONNECTION_KEEPALIVE_TIMEOUT = (MAPREDUCE_SHUFFLE_PREFIX + ".connection-keep-alive.timeout", 5)
    SHUFFLE_MAPOUTPUT_INFO_META_CACHE_SIZE = (MAPREDUCE_SHUFFLE_PREFIX + ".mapoutput-info.meta.cache.size", 1000)
    SHUFFLE_SSL_ENABLED = (MAPREDUCE_SHUFFLE_PREFIX + ".ssl.enabled", "false")
    SHUFFLE_PATHCACHE_EXPIRE_AFTER_ACCESS_MINUTES = (MAPREDUCE_SHUFFLE_PREFIX + ".pathcache.expire-after-access-minutes", 5)
    SHUFFLE_PATHCACHE_CONCURRENCY_LEVEL = (MAPREDUCE_SHUFFLE_PREFIX + ".pathcache.concurrency-level", 16)
    SHUFFLE_PATHCACHE_MAX_WEIGHT = (MAPREDUCE_SHUFFLE_PREFIX + ".pathcache.max-weight", 10 * 1024 * 1024)
    SHUFFLE_LOG_SEPARATE = (YARN_APP_MAPREDUCE_SHUFFLE_PREFIX + ".log.separate", "true")
    SHUFFLE_LOG_LIMIT_KB = (YARN_APP_MAPREDUCE_SHUFFLE_PREFIX + ".log.limit.kb", 0)
    SHUFFLE_LOG_BACKUPS = (YARN_APP_MAPREDUCE_SHUFFLE_PREFIX + ".log.backups", 0)

    def __init__(self, conf_key, default_val):
        self.conf_key = conf_key
        self.default_val = default_val


STORE_TYPE_JKS = "jks"
KEYSTORES_DIR = "/home/systest/keystores"
COMMON_TRUSTSTORE_LOCATION = f"{KEYSTORES_DIR}/truststore.jks"
COMMON_TRUSTSTORE_PASS = f"truststore_pass"


class SSLConfigWithDefault(Enum):
    CLIENT_TRUSTSTORE_TYPE = ("ssl.client.truststore.type", STORE_TYPE_JKS)
    CLIENT_TRUSTSTORE_LOCATION = ("ssl.client.truststore.location", COMMON_TRUSTSTORE_LOCATION)
    CLIENT_TRUSTSTORE_PASSWORD = ("ssl.client.truststore.password", COMMON_TRUSTSTORE_PASS)
    CLIENT_KEYSTORE_TYPE = ("ssl.client.keystore.type", STORE_TYPE_JKS)
    CLIENT_KEYSTORE_LOCATION = ("ssl.client.keystore.location", f"{KEYSTORES_DIR}/client-keystore.jks")
    CLIENT_KEYSTORE_PASSWORD = ("ssl.client.keystore.password", "ssl_client_ks_pass")

    SERVER_TRUSTSTORE_TYPE = ("ssl.server.truststore.type", STORE_TYPE_JKS)
    SERVER_TRUSTSTORE_LOCATION = ("ssl.server.truststore.location", COMMON_TRUSTSTORE_LOCATION)
    SERVER_TRUSTSTORE_PASSWORD = ("ssl.server.truststore.password", COMMON_TRUSTSTORE_PASS)
    SERVER_KEYSTORE_TYPE = ("ssl.server.keystore.type", STORE_TYPE_JKS)
    SERVER_KEYSTORE_LOCATION = ("ssl.server.keystore.location", f"{KEYSTORES_DIR}/server-keystore.jks")
    SERVER_KEYSTORE_PASSWORD = ("ssl.server.keystore.password", "ssl_server_ks_pass")

    HADOOP_REQUIRE_CLIENT_CERT = ("hadoop.ssl.require.client.cert", "false")
    HADOOP_HOSTNAME_VERIFIER = ("hadoop.ssl.hostname.verifier", "DEFAULT")
    HADOOP_SSL_KEYSTORES_FACTORY_CLASS = ("hadoop.ssl.keystores.factory.class", "org.apache.hadoop.security.ssl.FileBasedKeyStoresFactory")
    HADOOP_SSL_SERVER_CONF = ("hadoop.ssl.server.conf", "ssl-server.xml")
    HADOOP_SSL_CLIENT_CONF = ("hadoop.ssl.client.conf", "ssl-client.xml")

    def __init__(self, conf_key, default_val):
        self.conf_key = conf_key
        self.default_val = default_val
        
        
class ActualConfigs:
    @staticmethod
    def make_generic_conf_dict(config: 'Netty4TestConfig', confs: Dict[Any, str]):
        if config.generate_empty_ssl_configs:
            return {}
        result = {}
        for k, v in confs.items():
            if isinstance(k, SSLConfigWithDefault):
                result[k.conf_key] = v
            elif isinstance(k, str):
                result[k] = v
            else:
                raise ValueError("Unexpected key. Type: {}, key: {}, value: {}".format(type(k), k, v))
        return result

    @staticmethod
    def make_ssl_conf_dict(confs: List[SSLConfigWithDefault]):
        return {c.conf_key: c.default_val for c in confs}

    @staticmethod
    def make_conf_dict(confs_with_default_vals: List[ConfigWithDefault]):
        return {c.conf_key: c.default_val for c in confs_with_default_vals}

    def get_store_type_by_key(self, key: str) -> str:
        key = f"{key}.type"
        return self.STORE_SETTINGS["types"][key]

    def get_store_type(self, conf: SSLConfigWithDefault) -> str:
        return self.STORE_SETTINGS["types"][conf.conf_key]

    def get_store_password_by_key(self, key: str) -> str:
        key = f"{key}.password"
        return self.STORE_SETTINGS["passwords"][key]

    def get_store_password(self, conf: SSLConfigWithDefault) -> str:
        return self.STORE_SETTINGS["passwords"][conf.conf_key]

    def get_store_location_by_key(self, key: str) -> str:
        key = f"{key}.location"
        return self.STORE_SETTINGS["locations"][key]

    def get_store_location(self, conf: SSLConfigWithDefault) -> str:
        return self.STORE_SETTINGS["locations"][conf.conf_key]

    def __init__(self, config: 'Netty4TestConfig'):
        ssl = SSLConfigWithDefault
        c = ConfigWithDefault
        self.STORE_SETTINGS = {
            "passwords": self.make_ssl_conf_dict([ssl.SERVER_KEYSTORE_PASSWORD,
                                                  ssl.SERVER_TRUSTSTORE_PASSWORD,
                                                  ssl.CLIENT_KEYSTORE_PASSWORD,
                                                  ssl.CLIENT_TRUSTSTORE_PASSWORD
                                                  ]),
            "locations": self.make_ssl_conf_dict([
                ssl.SERVER_KEYSTORE_LOCATION,
                ssl.SERVER_TRUSTSTORE_LOCATION,
                ssl.CLIENT_KEYSTORE_LOCATION,
                ssl.CLIENT_TRUSTSTORE_LOCATION
            ]),
            "types": self.make_ssl_conf_dict([
                ssl.SERVER_KEYSTORE_TYPE,
                ssl.SERVER_TRUSTSTORE_TYPE,
                ssl.CLIENT_KEYSTORE_TYPE,
                ssl.CLIENT_TRUSTSTORE_TYPE,
            ])
        }

        self.DEFAULT_MAPRED_SITE_CONFIGS: Dict[str, str] = self.make_conf_dict(
            [c.SHUFFLE_MANAGE_OS_CACHE,
             c.SHUFFLE_READAHEAD_BYTES,
             c.SHUFFLE_MAX_CONNECTIONS,
             c.SHUFFLE_MAX_THREADS,
             c.SHUFFLE_TRANSFER_BUFFER_SIZE,
             c.SHUFFLE_TRANSFERTO_ALLOWED,
             c.SHUFFLE_MAX_SESSION_OPEN_FILES,
             c.SHUFFLE_LISTEN_QUEUE_SIZE,
             c.SHUFFLE_PORT,
             c.SHUFFLE_SSL_FILE_BUFFER_SIZE,
             c.SHUFFLE_CONNECTION_KEEPALIVE_ENABLE,
             c.SHUFFLE_CONNECTION_KEEPALIVE_TIMEOUT,
             c.SHUFFLE_MAPOUTPUT_INFO_META_CACHE_SIZE,
             c.SHUFFLE_SSL_ENABLED,
             c.SHUFFLE_PATHCACHE_EXPIRE_AFTER_ACCESS_MINUTES,
             c.SHUFFLE_PATHCACHE_CONCURRENCY_LEVEL,
             c.SHUFFLE_PATHCACHE_MAX_WEIGHT,
             c.SHUFFLE_LOG_SEPARATE,
             c.SHUFFLE_LOG_LIMIT_KB,
             c.SHUFFLE_PATHCACHE_MAX_WEIGHT,
             c.SHUFFLE_LOG_BACKUPS,
             ]
        )

        self.DEFAULT_CORE_SITE_CONFIGS = self.make_ssl_conf_dict([
            ssl.HADOOP_REQUIRE_CLIENT_CERT,
            ssl.HADOOP_HOSTNAME_VERIFIER,
            ssl.HADOOP_SSL_KEYSTORES_FACTORY_CLASS,
            ssl.HADOOP_SSL_SERVER_CONF,
            ssl.HADOOP_SSL_CLIENT_CONF
        ])

        self.DEFAULT_SSL_SERVER_CONFIGS = self.make_generic_conf_dict(config, {
            ssl.SERVER_KEYSTORE_TYPE: self.get_store_type(ssl.SERVER_KEYSTORE_TYPE),
            ssl.SERVER_KEYSTORE_LOCATION: self.get_store_location(ssl.SERVER_KEYSTORE_LOCATION),
            ssl.SERVER_KEYSTORE_PASSWORD: self.get_store_password(ssl.SERVER_KEYSTORE_PASSWORD),
            ssl.SERVER_TRUSTSTORE_TYPE: self.get_store_type(ssl.SERVER_TRUSTSTORE_TYPE),
            ssl.SERVER_TRUSTSTORE_LOCATION: self.get_store_location(ssl.SERVER_TRUSTSTORE_LOCATION),
            ssl.SERVER_TRUSTSTORE_PASSWORD: self.get_store_password(ssl.SERVER_TRUSTSTORE_PASSWORD),
            "ssl.server.truststore.reload.interval": "10000"
        })

        self.DEFAULT_SSL_CLIENT_CONFIGS = self.make_generic_conf_dict(config, {
            ssl.CLIENT_KEYSTORE_TYPE: self.get_store_type(ssl.CLIENT_KEYSTORE_TYPE),
            ssl.CLIENT_KEYSTORE_LOCATION: self.get_store_location(ssl.CLIENT_KEYSTORE_LOCATION),
            ssl.CLIENT_KEYSTORE_PASSWORD: self.get_store_password(ssl.CLIENT_KEYSTORE_PASSWORD),
            ssl.CLIENT_TRUSTSTORE_TYPE: self.get_store_type(ssl.CLIENT_TRUSTSTORE_TYPE),
            ssl.CLIENT_TRUSTSTORE_LOCATION: self.get_store_location(ssl.CLIENT_TRUSTSTORE_LOCATION),
            ssl.CLIENT_TRUSTSTORE_PASSWORD: self.get_store_password(ssl.CLIENT_TRUSTSTORE_PASSWORD),
            "ssl.client.truststore.reload.interval": "10000"
        })

        self.DEFAULT_YARN_ENV_SH_CONFIGS = {}
        if config.enable_ssl_debugging:
            self.DEFAULT_YARN_ENV_SH_CONFIGS["YARN_NODEMANAGER_OPTS"] = "-Djavax.net.debug=all"


APP_LOG_FILE_NAME_FORMAT = "app_{app}.log"
YARN_LOG_FILE_NAME_FORMAT = "{host}_{role}_{app}.log"
PREFIXED_YARN_LOG_FILE_NAME_FORMAT = "{prefix}_{host}_{role}.log"
YARN_LOG_FORMAT = "{name} - {log}"
CONF_FORMAT = "{host}_{conf}"


def _callback(cmd: RunnableCommand, logs_dict: Dict[RunnableCommand, List[str]]) -> Callable:
    def _cb(line: str):
        if cmd not in logs_dict or not logs_dict[cmd]:
            logs_dict[cmd] = []
        logs_dict[cmd].append(YARN_LOG_FORMAT.format(name=cmd.target.host, log=line))
        # LOG.debug("****logs_dict: %s", logs_dict)

    return _cb


class SSLSetupMode(Enum):
    BETWEEN_EACH_TESTCASE = "between_each_testcase"
    ONCE = "once"
    SKIP = "skip"


class Netty4TestcasesBuilder:
    def __init__(self, name):
        self.configs: Dict[str, List[str]] = {}
        self.name = name
        self.apps: List[MapReduceAppType] = []
        self.ssl_based = False

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

    def with_ssl_based(self, value: bool):
        self.ssl_based = value
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
                testcases.append(Netty4Testcase(self.name, self._generate_tc_name(tc_counter, app_type), conf_changes,
                                                config.mr_apps[app_type],
                                                ssl_based=self.ssl_based))
        return testcases

    def _generate_tc_name(self, tc_counter, app_type: MapReduceAppType):
        return f"{self.name}_{str(tc_counter)}_{app_type.value}"


@dataclass
class LogLevelSpec:
    selector: str
    package: str
    log_level: str


@dataclass
class Netty4Testcase:
    simple_name: str
    name: str
    config_changes: Dict[str, str]
    app: MapReduceApp
    ssl_based: bool = False

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
    def __init__(self, cluster_handler):
        self.cluster_handler = cluster_handler
        self._generated_files = GeneratedOutputFiles()
        self.tc_no = 0
        self.workdir = None
        self.context = None
        self.tc = None
        self.current_ctx_dir = None
        self.current_tc_dir = None

        self.initial_config_files: List[Tuple[OutputFileType, HadoopConfigFile]] = [
            (OutputFileType.INITIAL_CONFIG_MR, HadoopConfigFile.MAPRED_SITE),
            (OutputFileType.INITIAL_CONFIG_YARN_SITE, HadoopConfigFile.YARN_SITE),
            (OutputFileType.INITIAL_CONFIG_CORE_SITE, HadoopConfigFile.CORE_SITE),
            (OutputFileType.INITIAL_CONFIG_SSL_SERVER, HadoopConfigFile.SSL_SERVER),
            (OutputFileType.INITIAL_CONFIG_SSL_CLIENT, HadoopConfigFile.SSL_CLIENT),
            (OutputFileType.INITIAL_CONFIG_LOG4J_PROPERTIES, HadoopConfigFile.LOG4J_PROPERTIES),
        ]

        self.testcase_config_files: List[Tuple[OutputFileType, HadoopConfigFile]] = [
            (OutputFileType.TC_CONFIG_MR, HadoopConfigFile.MAPRED_SITE),
            (OutputFileType.TC_CONFIG_YARN_SITE, HadoopConfigFile.YARN_SITE),
            (OutputFileType.TC_CONFIG_CORE_SITE, HadoopConfigFile.CORE_SITE),
            (OutputFileType.TC_CONFIG_SSL_SERVER, HadoopConfigFile.SSL_SERVER),
            (OutputFileType.TC_CONFIG_SSL_CLIENT, HadoopConfigFile.SSL_CLIENT),
            (OutputFileType.TC_CONFIG_LOG4J_PROPERTIES, HadoopConfigFile.LOG4J_PROPERTIES),
        ]

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

    def _write_config_files(self, selector: str, conf_type: HadoopConfigFile, conf_dir=None) -> List[str]:
        conf_type_str = ""
        if conf_dir == CONF_DIR_INITIAL:
            conf_type_str = "initial"
        elif conf_dir == CONF_DIR_TC:
            conf_type_str = "testcase"
        LOG.info("Writing %s files for selector '%s'", conf_type_str, selector)

        configs = self.cluster_handler.get_config(selector, conf_type)
        generated_config_files = []
        for host, conf in configs.items():
            config_file_name = CONF_FORMAT.format(host=host, conf=conf_type.val)
            if "." not in config_file_name:
                raise ValueError("Invalid config file name: {}. File does not have extension!".format(config_file_name))
            if conf_dir:
                dir_path = os.path.join(self.current_tc_dir, conf_dir)
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
        for config_tup in self.initial_config_files:
            self._generated_files.register_files(config_tup[0],
                                                 self._write_config_files(NODEMANAGER_SELECTOR,
                                                                          config_tup[1],
                                                                          conf_dir=CONF_DIR_INITIAL))

    def write_testcase_config_files(self):
        for config_tup in self.testcase_config_files:
            self._generated_files.register_files(config_tup[0],
                                                 self._write_config_files(NODEMANAGER_SELECTOR,
                                                                          config_tup[1],
                                                                          conf_dir=CONF_DIR_TC))

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

        for node_id, dic in node_health_reports.items():
            chars = [(".", "_"), (":", "_"), ("-", "_")]
            new_node_id = StringUtils.replace_chars(node_id, chars)
            file_path = os.path.join(self.current_tc_dir, f"healthreport_{new_node_id}.txt")
            files_written.append(file_path)
            with open(file_path, 'w') as f:
                f.writelines(dic)
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
        try:
            files = self.cluster_handler.save_app_logs_from_cluster(app_id, self.current_tc_dir, self.save_yarn_daemon_logs_callback)
            if files:
                self._generated_files.register_files(OutputFileType.APP_LOG_TAR_GZ, files)
        except HadesException as he:
            self.write_node_health_reports(self.cluster_handler.get_state_and_health_report())
            raise he
        # TODO Node health report contains only 1 line
        self.write_node_health_reports(self.cluster_handler.get_state_and_health_report())

    def save_yarn_daemon_logs_callback(self, files):
        self._generated_files.register_files(OutputFileType.YARN_DAEMON_LOGS_TAR_GZ, files)

    def write_yarn_app_logs(self, app_name, app_command, app_log_lines):
        app_log_file = APP_LOG_FILE_NAME_FORMAT.format(app=app_name)
        app_log_file_path = os.path.join(self.current_tc_dir, app_log_file)

        LOG.debug("Writing app log file '%s' on host '%s'", app_log_file_path, app_command.target.host)
        with open(app_log_file_path, 'w') as f:
            f.writelines(app_log_lines)
        self._generated_files.register_files(OutputFileType.APP_LOG_FILE, [app_log_file])

    def decompress_app_container_logs(self, remove_src_file=True):
        self._decompress_logs(OutputFileType.APP_LOG_TAR_GZ, OutputFileType.EXTRACTED_APP_LOG_FILES,
                              remove_src_file=remove_src_file)

    def decompress_daemon_logs(self, remove_src_file=True):
        self._decompress_logs(OutputFileType.YARN_DAEMON_LOGS_TAR_GZ, OutputFileType.EXTRACTED_YARN_DAEMON_LOG_FILES,
                              remove_src_file=remove_src_file)

    def _decompress_logs(self, src_type: 'OutputFileType', dst_type: 'OutputFileType',
                         remove_src_file=False):
        for tar_file in self._generated_files.get(src_type):
            target_dir = os.path.join(self.current_tc_dir)
            LOG.debug("[%s] Extracting file '%s' to %s", self.context, tar_file, target_dir)
            CompressedFileUtils.extract_targz_file(tar_file, target_dir)
            self._generated_files.register_files(dst_type,
                                                 CompressedFileUtils.list_targz_file(tar_file),
                                                 allow_multiple=True)
            if remove_src_file and os.path.isfile(tar_file):
                LOG.debug("Removing and de-registering file: %s", tar_file)
                os.remove(tar_file)
                self._generated_files.deregister_file(src_type, tar_file)

    def write_patch_file(self, patch_file):
        target_file = os.path.join(self.current_ctx_dir, os.path.basename(patch_file))
        patch_file = os.path.expanduser(patch_file)
        shutil.copyfile(patch_file, target_file)
        self._generated_files.register_files(OutputFileType.PATCH_FILE, [target_file])


@dataclass
class LogsByRoles:
    output_file_writer: 'OutputFileWriter'
    selector: str
    roles = None
    log_commands = None

    def __post_init__(self):
        self.log_lines_dict = {}

    def search_in_logs(self, verification: LogVerification):
        LOG.info("Searching in logs, verification: %s", verification)
        filtered_roles = list(
            filter(lambda rc: rc.target.role_type in [verification.role_type], list(self.log_lines_dict.keys())))

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
    ssl_setup_completed: bool = False

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
    # only_run_testcases = ["shuffle_ssl_enabled"]
    only_run_testcases = []
    LIMIT_TESTCASES = False
    QUICK_MODE = False
    # testcase_limit = 1 if QUICK_MODE or LIMIT_TESTCASES else TC_LIMIT_UNLIMITED
    testcase_limit = 2
    enable_compilation = False if QUICK_MODE else True
    allow_verification_failure = True if QUICK_MODE else False

    mr_app_debug = False
    timeout_for_apps = 120
    compress_tc_result = False
    decompress_app_container_logs = True
    decompress_daemon_logs = True
    shufflehandler_log_level = HadoopLogLevel.DEBUG
    cache_built_maven_artifacts = True
    halt_execution_on_failed_job = True
    halt_execution_on_job_timeout = False
    loadgen_no_mappers = 4
    loadgen_no_reducers = 3
    loadgen_timeout = 1000
    run_without_patch = False
    run_with_patch = True
    enable_ssl_debugging = False
    generate_empty_ssl_configs = False
    ssl_setup_mode = SSLSetupMode.BETWEEN_EACH_TESTCASE
    # TODO Implement switch that simulates an intentional job failure for given testcase names e.g. 'shuffle_ssl_enabled'
    patch_file_path = str(Path.home()) + os.sep + "netty4patch.patch"
    netty_log_message = "*** HADOOP-15327: netty upgrade"

    force_compile = False
    sleep_after_service_restart = 15

    def __post_init__(self):
        sleep_job = MapReduceApp(MapReduceAppType.SLEEP, cmd='sleep -m 1 -r 1 -mt 10 -rt 10', timeout=self.timeout_for_apps, debug=self.mr_app_debug)
        pi_job = MapReduceApp(MapReduceAppType.PI, cmd='pi 1 1000', timeout=self.timeout_for_apps, debug=self.mr_app_debug)
        loadgen_job = MapReduceApp(MapReduceAppType.LOADGEN,
                                   cmd=f"loadgen -m {self.loadgen_no_mappers} -r {self.loadgen_no_reducers} "
                                       f"-outKey org.apache.hadoop.io.Text "
                                       f"-outValue org.apache.hadoop.io.Text",
                                   timeout=self.loadgen_timeout,
                                   debug=self.mr_app_debug)

        sort_input_dir = "/user/systest/sortInputDir"
        sort_output_dir = "/user/systest/sortOutputDir"
        random_writer_job = MapReduceApp(MapReduceAppType.RANDOM_WRITER, cmd=f"randomwriter {sort_input_dir}", debug=self.mr_app_debug)
        mapred_sort_job = MapReduceApp(MapReduceAppType.TEST_MAPRED_SORT,
                                       cmd=f"testmapredsort "
                                           f"-sortInput {sort_input_dir} "
                                           f"-sortOutput {sort_output_dir}",
                                       debug=self.mr_app_debug)

        self.mr_apps: Dict[MapReduceAppType, MapReduceApp] = {
            MapReduceAppType.SLEEP: sleep_job,
            MapReduceAppType.PI: pi_job,
            MapReduceAppType.TEST_MAPRED_SORT: mapred_sort_job,
            MapReduceAppType.RANDOM_WRITER: random_writer_job,
            MapReduceAppType.LOADGEN: loadgen_job
        }
        self.default_apps = [MapReduceAppType.SLEEP, MapReduceAppType.LOADGEN]

        self.contexts = []
        if self.run_without_patch:
            self.contexts.append(Netty4TestContext("without netty patch on trunk",
                                                   DEFAULT_BRANCH,
                                                   log_verifications=[
                                                       LogVerification(HadoopRoleType.NM, self.netty_log_message,
                                                                       inverted_mode=True)],
                                                   compile=self.enable_compilation or self.force_compile,
                                                   allow_verification_failure=self.allow_verification_failure))
        if self.run_with_patch:
            if not os.path.exists(self.patch_file_path):
                raise ValueError("Patch file cannot be found in: {}".format(self.patch_file_path))
            self.contexts.append(Netty4TestContext("with netty patch based on trunk",
                                                   DEFAULT_BRANCH,
                                                   patch_file=self.patch_file_path,
                                                   log_verifications=[
                                                       LogVerification(HadoopRoleType.NM, self.netty_log_message,
                                                                       inverted_mode=False)],
                                                   compile=self.enable_compilation or self.force_compile,
                                                   allow_verification_failure=self.allow_verification_failure
                                                   ))

        self.testcases = [
            *Netty4TestcasesBuilder("shuffle_max_connections")
            .with_configs(ConfigWithDefault.SHUFFLE_MAX_CONNECTIONS.conf_key, ["5", "10"])
            .with_apps(self.default_apps)
            .generate_testcases(self),
            *Netty4TestcasesBuilder("shuffle_max_threads")
            .with_configs(ConfigWithDefault.SHUFFLE_MAX_THREADS.conf_key, ["3", "6"])
            .with_apps(self.default_apps)
            .generate_testcases(self),
            *Netty4TestcasesBuilder("shuffle_max_open_files")
            .with_configs(ConfigWithDefault.SHUFFLE_MAX_SESSION_OPEN_FILES.conf_key, ["2", "5"])
            .with_apps(self.default_apps)
            .generate_testcases(self),
            *Netty4TestcasesBuilder("shuffle_listen_queue_size")
            .with_configs(ConfigWithDefault.SHUFFLE_LISTEN_QUEUE_SIZE.conf_key, ["10", "50"])
            .with_apps(self.default_apps)
            .generate_testcases(self),
            *Netty4TestcasesBuilder("shuffle_ssl_enabled")
            .with_configs(ConfigWithDefault.SHUFFLE_SSL_ENABLED.conf_key, ["true"])
            .with_apps(self.default_apps)
            .with_ssl_based(True)
            .generate_testcases(self),
            *Netty4TestcasesBuilder("keepalive")
            .with_config(ConfigWithDefault.SHUFFLE_CONNECTION_KEEPALIVE_ENABLE.conf_key, "true")
            .with_configs(ConfigWithDefault.SHUFFLE_CONNECTION_KEEPALIVE_TIMEOUT.conf_key, ["15", "25"])
            .with_apps(self.default_apps)
            .generate_testcases(self)
        ]

        if self.only_run_testcases:
            filtered_testcases = []
            for tc in self.testcases:
                if tc.simple_name in self.only_run_testcases:
                    filtered_testcases.append(tc)

            diff = set(self.only_run_testcases).difference(set([tc.simple_name for tc in self.testcases]))
            if diff:
                raise HadesException("Cannot find testcases for these specified testcases '{}'. All specified testcases: {}".format(diff, self.only_run_testcases))

            LOG.info("Found testcases matching for names '%s': %s", self.only_run_testcases, filtered_testcases)
            self.testcases = filtered_testcases


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
        if self.context not in self.results:
            self.results[self.context] = {}
        self.tc = testcase

    def update_with_result(self, tc, result):
        LOG.info("Result of testcase '%s' is: %s", tc, result)
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
    INITIAL_CONFIG_SSL_CLIENT = "initial_config_ssl_client"
    INITIAL_CONFIG_LOG4J_PROPERTIES = "initial_config_log4j_properties"

    TC_CONFIG_MR = "testcase_config_mr"
    TC_CONFIG_YARN_SITE = "testcase_config_yarn_site"
    TC_CONFIG_CORE_SITE = "testcase_config_core_site"
    TC_CONFIG_SSL_SERVER = "testcase_config_ssl_server"
    TC_CONFIG_SSL_CLIENT = "testcase_config_ssl_client"
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
        self.expected_not_empty_output_types = [
            (OutputFileType.TC_CONFIG_MR, HadesException, "Expected non-empty testcase config mapred-site.xml files list!"),
            (OutputFileType.TC_CONFIG_YARN_SITE, HadesException, "Expected non-empty testcase config yarn-site.xml files list!"),
            (OutputFileType.INITIAL_CONFIG_MR, HadesException, "Expected non-empty initial config files list!"),
            (OutputFileType.YARN_DAEMON_LOGS, HadesException, "Expected non-empty YARN log files list!"),
            (OutputFileType.NM_RESTART_LOGS, HadesException, "Expected non-empty YARN NM restart log files!"),
        ]

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

    def deregister_file(self, out_type: OutputFileType, file: str):
        self._curr_files_dict[out_type].remove(file)

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
        for tup in self.expected_not_empty_output_types:
            if not self.get(tup[0]):
                raise tup[1](tup[2])

        if app_failed and not self.get(OutputFileType.YARN_DAEMON_LOGS_TAR_GZ):
            raise HadesException("App failed. Expected non-empty daemon logs for YARN processes!")

        if not app_failed and (
                not self.get(OutputFileType.YARN_DAEMON_LOGS_TAR_GZ) or not self.get(OutputFileType.APP_LOG_TAR_GZ)):
            raise HadesException(
                "App not failed. Expected non-empty daemon logs for YARN processes and app log tar files list!")


@dataclass
class CompilationContext:
    branch: str
    commit_hash: str
    patch_file: str
    changed_modules: Dict[str, str]
    timestamp: int = None
    patch_file_hash: str or None = None

    def __post_init__(self):
        if not self.patch_file:
            self.patch_file = "without_patch"
        self.patch_file_hash = FileUtils.get_hash_of_file(self.patch_file)
        self.timestamp = int(time.time())


@dataclass
class ModuleCacheDbKey:
    patch_file_hash: str
    commit_hash: str
    module_name: str

    @staticmethod
    def create(ctx: CompilationContext, module_name: str):
        return ModuleCacheDbKey(ctx.patch_file_hash, ctx.commit_hash, module_name)

    def __key(self):
        # key: hash of patch file + commit hash
        return self.patch_file_hash, self.commit_hash, self.module_name

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, ModuleCacheDbKey):
            return self.__key() == other.__key()
        return NotImplemented


class ModuleCacheDb:
    def __init__(self):
        # value: module file path
        self._modules: Dict[ModuleCacheDbKey, str] = {}

    def add_module(self, db_key: ModuleCacheDbKey, module_path: str):
        self._modules[db_key] = module_path

    def get_module(self, db_key: ModuleCacheDbKey) -> str or None:
        if db_key in self._modules:
            return self._modules[db_key]
        return None


class CompiledModuleCache:
    def __init__(self, workdir, hadoop_path):
        self.workdir = workdir
        self._hadoop_path = hadoop_path
        self._db_file = os.path.join(self.workdir, "moduledb", "db.pickle")
        self._db: ModuleCacheDb = None

    def load(self):
        self._db = self.load_db_file()

    def check_cache_hit(self, ctx: CompilationContext):
        found_modules: Dict[str, str] = {}
        for module_name, module_path in ctx.changed_modules.items():
            found_module_path = self._db.get_module(ModuleCacheDbKey.create(ctx, module_name))
            if not found_module_path or not os.path.exists(found_module_path):
                LOG.info("Module '%s' not found in cache", module_name)
                return False, found_modules
            else:
                found_modules[module_name] = found_module_path

        return True, found_modules

    def save_modules(self, ctx: CompilationContext):
        cached_modules: Dict[str, str] = self._build_cache_path_for_modules(ctx)
        modules_in_cache = {}

        for module, module_path in cached_modules.items():
            modules_in_cache[module] = module_path
            FileUtils.ensure_dir_created(os.path.dirname(module_path))
            shutil.copyfile(ctx.changed_modules[module], module_path)
            db_key = ModuleCacheDbKey.create(ctx, module)
            self._db.add_module(db_key, module_path)

        self.write_db_file()

    def _build_cache_path_for_modules(self, ctx: CompilationContext) -> Dict[str, str]:
        cache_paths: Dict[str, str] = {}
        for module_name, module_path in ctx.changed_modules.items():
            module_path = module_path.replace(self._hadoop_path, "").lstrip(os.sep)
            cache_paths[module_name] = os.path.join(self.workdir, "modulecache", str(ctx.timestamp), module_path)
        return cache_paths

    def load_db_file(self):
        LOG.debug("Loading DB from file: %s", self._db_file)
        if os.path.exists(self._db_file):
            with open(self._db_file, "rb") as db:
                return pickle.load(db)

        # Create empty DB
        return ModuleCacheDb()

    def write_db_file(self):
        LOG.debug("Writing DB to file: %s", self._db_file)
        FileUtils.ensure_dir_created(os.path.dirname(self._db_file))
        with open(self._db_file, "wb") as db:
            pickle.dump(self._db, db)


class Compiler:
    def __init__(self, workdir, context, handler, config):
        self.workdir = workdir
        self.context = context
        self.handler = handler
        self.config = config
        self._cache = CompiledModuleCache(self.workdir, self.handler.ctx.config.hadoop_path)
        self._use_cache = self.config.cache_built_maven_artifacts

        if self._use_cache:
            self._cache.load()

    def compile(self, expect_changed_modules=False, force_compile_if_no_changed_modules=True):
        LOG.info("Compile set to %s, force compile: %s", self.context.compile, self.config.force_compile)
        if self.context.compile:
            hadoop_dir = HadoopDir(self.handler.ctx.config.hadoop_path)
            compilation_required = True
            all_loaded = False

            comp_context = CompilationContext(branch=hadoop_dir.get_current_branch(fallback="trunk"),
                                              commit_hash=hadoop_dir.get_current_commit_hash(),
                                              patch_file=self.context.patch_file,
                                              changed_modules=hadoop_dir.get_changed_module_paths())

            if not self.config.force_compile and self._use_cache:
                hadoop_dir.extract_changed_modules(allow_empty=True)
                if expect_changed_modules and not comp_context.changed_modules:
                    if not force_compile_if_no_changed_modules:
                        raise HadesException("Expected changed modules but changed modules not found!")
                    else:
                        compilation_required = True

                all_loaded, cached_modules = self._cache.check_cache_hit(comp_context)
                if all_loaded:
                    compilation_required = False
                    LOG.info("Found all required modules in the cache, compilation won't be performed! Modules: %s", cached_modules)

            if compilation_required:
                LOG.info("[%s] Starting compilation...", self.context)
                self.handler.compile(all=True,
                                     changed=False,
                                     deploy=True,
                                     modules=None,
                                     no_copy=True,
                                     single=None)
                if self._use_cache:
                    self._cache.save_modules(comp_context)
            elif expect_changed_modules and all_loaded:
                # Deploy even if we did not perform compilation
                self.handler.deploy()


class ExecutionState(Enum):
    RUNNING = "running"
    HALTED = "halted"


class Netty4RegressionTestSteps:
    def __init__(self, test, no_of_testcases, handler):
        self.overwrite_config_func = test.overwrite_config
        self.no_of_testcases = no_of_testcases
        self.handler = handler
        self.cluster_handler = ClusterHandler(test.cluster)
        self.cluster_config_updater = ClusterConfigUpdater(self.cluster_handler, test.session_dir)
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
        self.actual_configs = ActualConfigs(self.config)

    def start_context(self, context):
        if self._should_halt():
            status = self.test_results.current_result.type.value
            LOG.info("Execution halted as status of last job is: %s", status)
            self.execution_state = ExecutionState.HALTED
            return self.execution_state

        self.context = context
        LOG.info("Starting: %s", self.context)
        PrintUtils.print_banner_figlet(f"STARTING CONTEXT: {self.context}")
        self.output_file_writer = OutputFileWriter(self.cluster_handler)
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
            return True
        return False

    @staticmethod
    def _validate_config_values(config_file: HadoopConfigFile, configs: Dict[str, str]):
        invalid = {}
        for k, v in configs.items():
            if v == INVALID_CONFIG_VALUE:
                invalid[k] = v

        if invalid:
            raise HadesException(
                "Invalid configuration entries found for '{}'. Entries: {}".format(config_file.config_type, invalid))

    def _load_configs(self, log_msg,
                      configs: Dict[str, str],
                      config_file: HadoopConfigFile,
                      selector: str,
                      allow_empty_configs: bool = False,
                      delete_configs: Dict[str, str] = None):
        LOG.info(log_msg)
        self._validate_config_values(config_file, configs)
        self.cluster_config_updater.load_configs(config_file,
                                                 configs,
                                                 selector,
                                                 allow_empty=allow_empty_configs,
                                                 delete_configs=delete_configs)

    def load_default_yarn_site_configs(self):
        self._load_configs(log_msg="Loading default yarn-site.xml configs for NodeManagers...",
                           configs=ClusterConfigUpdater.YARN_SITE_DEFAULT_CONFIGS,
                           config_file=HadoopConfigFile.YARN_SITE,
                           selector=NODEMANAGER_SELECTOR)

    def load_default_mapred_configs(self):
        configs = dict(self.actual_configs.DEFAULT_MAPRED_SITE_CONFIGS)
        delete_configs = {}
        if self.config.enable_ssl_debugging:
            configs["mapred.reduce.child.java.opts"] = "-Djavax.net.debug=all"
        else:
            # Delete this config if it's a remnant from a previous run to really switch off SSL debugging
            delete_configs["mapred.reduce.child.java.opts"] = "-Djavax.net.debug=all"

        self._load_configs(log_msg="Loading default MR ShuffleHandler configs for NodeManagers...",
                           configs=configs,
                           delete_configs=delete_configs,
                           config_file=HadoopConfigFile.MAPRED_SITE,
                           selector=NODEMANAGER_SELECTOR)

    def load_default_core_site_configs(self):
        self._load_configs(log_msg="Loading default core-site.xml configs for ResourceManager and NodeManagers...",
                           configs=self.actual_configs.DEFAULT_CORE_SITE_CONFIGS,
                           config_file=HadoopConfigFile.CORE_SITE,
                           selector=YARN_SELECTOR)

    def load_default_ssl_server_configs(self):
        self._load_configs(log_msg="Loading default ssl-server.xml configs for ResourceManager and NodeManagers...",
                           configs=self.actual_configs.DEFAULT_SSL_SERVER_CONFIGS,
                           config_file=HadoopConfigFile.SSL_SERVER,
                           selector=YARN_SELECTOR,
                           allow_empty_configs=True)

    def load_default_ssl_client_configs(self):
        self._load_configs(log_msg="Loading default ssl-client.xml configs for ResourceManager and NodeManagers...",
                           configs=self.actual_configs.DEFAULT_SSL_CLIENT_CONFIGS,
                           config_file=HadoopConfigFile.SSL_CLIENT,
                           selector=YARN_SELECTOR,
                           allow_empty_configs=True)

    def load_default_yarn_env_sh_config(self):
        delete_configs = {}
        if not self.config.enable_ssl_debugging:
            # Delete this config if it's a remnant from a previous run to really switch off SSL debugging
            delete_configs["YARN_NODEMANAGER_OPTS"] = "-Djavax.net.debug=all"

        self._load_configs(log_msg="Loading default yarn-env.sh configs for NodeManagers...",
                           configs=self.actual_configs.DEFAULT_YARN_ENV_SH_CONFIGS,
                           delete_configs=delete_configs,
                           config_file=HadoopConfigFile.YARN_ENV_SH,
                           selector=YARN_SELECTOR)

    def init_testcase(self, tc):
        if self._should_halt():
            self.execution_state = ExecutionState.HALTED
            return self.execution_state

        self.tc = tc
        self.test_results.update_with_context_and_testcase(self.context, self.tc)
        self.output_file_writer.update_with_context_and_testcase(self.session_dir, self.context, self.tc)
        LOG.info("[%s] [%d / %d] Running testcase: %s", self.context, self.output_file_writer.tc_no,
                 self.no_of_testcases,
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
        self.load_default_ssl_client_configs()
        self.load_default_yarn_env_sh_config()
        self.hadoop_config = HadoopConfigBase.create(HadoopConfigFile.MAPRED_SITE)
        self.output_file_writer.write_initial_config_files()

    def apply_testcase_configs(self):
        LOG.info("Applying testcase configs...")
        for config_key, config_val in self.tc.config_changes.items():
            self.hadoop_config.extend_with_args({config_key: config_val})

        self.cluster_handler.update_config(NODEMANAGER_SELECTOR, self.hadoop_config, no_backup=True, workdir=self.session_dir)

    def set_log_levels(self, permanent=True):
        LOG.debug("Setting ShuffleHandler log level to: %s", self.config.shufflehandler_log_level)
        if permanent:
            LOG.info("Loading default log4j.properties configs for NodeManagers...")
            configs = {f"log4j.logger.{PACKAGE_SHUFFLEHANDLER}": self.config.shufflehandler_log_level.value,
                       f"log4j.logger.{PACKAGE_SECURITY_SSL}": HadoopLogLevel.DEBUG.value}
            self.cluster_config_updater.load_properties_configs(HadoopConfigFile.LOG4J_PROPERTIES,
                                                                configs,
                                                                YARN_SELECTOR,
                                                                allow_empty=False)
        else:
            self._set_log_levels_via_daemonlog()

    def _set_log_levels_via_daemonlog(self):
        self.cluster_handler.set_log_levels_via_daemonlog([
            LogLevelSpec(YARN_SELECTOR, PACKAGE_SHUFFLEHANDLER, self.config.shufflehandler_log_level.value),
            LogLevelSpec(YARN_SELECTOR, PACKAGE_SECURITY_SSL, HadoopLogLevel.DEBUG.value)
        ])

    def verify_log_levels(self, levels: List[Tuple[str, HadoopLogLevel]]):
        levels_dict = {tup[0]: tup[1] for tup in levels}
        LOG.debug("Verifying expected log levels: %s", levels_dict)

        cmd_dict: Dict[str, List[RunnableCommand]] = self.cluster_handler.get_log_levels(
            selector=YARN_SELECTOR,
            packages=list(levels_dict.keys())
        )

        LOG.debug("YARN get log level commands: %s", cmd_dict)
        if not cmd_dict:
            raise HadesException("No 'get log levels' commands were created!")

        bad_log_levels: Dict[str, List[Tuple[str, str]]] = {}
        for package, cmds in cmd_dict.items():
            for cmd in cmds:
                LOG.debug("Running command '%s' in async mode on host '%s'", cmd.cmd, cmd.target.host)
                cmd.run()

                stdout = "\n".join(cmd.stdout)
                regex = "Effective Level: (.*)"
                result = re.findall(f".*{regex}", stdout)
                if len(result) != 1:
                    raise HadesException(
                        f"Unexpected loglevel output, cannot find regex \"{regex}\"! Command: {cmd.cmd}, Output: {stdout}, Host: {cmd.target.host}")
                effective_level = result[0]
                if effective_level != levels_dict[package].value:
                    if cmd.target.host not in bad_log_levels:
                        bad_log_levels[cmd.target.host] = []
                    bad_log_levels[cmd.target.host].append((package, effective_level))
        if bad_log_levels:
            raise HadesException("Unexpected log levels: {}".format(bad_log_levels))

    def restart_services_and_save_logs(self, sleep_after: int = 0):
        LOG.info("Restarting NodeManagers with new configs...")
        self.nm_restart_logs = LogsByRoles(self.output_file_writer, selector=NODEMANAGER_SELECTOR)
        self.cluster_handler.restart_nms(self.nm_restart_logs)
        self.output_file_writer.write_nm_restart_logs(self.nm_restart_logs)
        LOG.info("Restarting ResourceManager...")
        self.cluster_handler.restart_rm(sleep_after=sleep_after)

    def run_app_and_collect_logs_to_file(self, app: MapReduceApp):
        app_log_lines = []
        with self.overwrite_config_func(cmd_prefix="sudo -u systest"):
            app_command, result_type = self.cluster_handler.run_app(app, app_log_lines, selector=NODEMANAGER_SELECTOR)

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
        self.yarn_logs = LogsByRoles(self.output_file_writer, selector=YARN_SELECTOR)
        self.cluster_handler.read_logs_into_dict(self.yarn_logs)

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
                    self.nm_restart_logs.search_in_logs(verification)
            except HadesException as e:
                if self.context.allow_verification_failure:
                    LOG.exception("Verification failed: %s, but allow verification failure is set to True!",
                                  verification)
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
        self.cluster_handler.save_yarn_daemon_logs(self.output_file_writer.current_tc_dir, self.output_file_writer.save_yarn_daemon_logs_callback)
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
            LOG.warning("Skipping comparison of testcase results as the testcase limit is set to: %s",
                        self.config.testcase_limit)
            return
        self.test_results.compare(self.config.contexts[0])

    def compile(self, expect_changed_modules=False):
        self.compiler.compile(expect_changed_modules=expect_changed_modules)

    def verify_nm_configs(self, dic: Dict[HadoopConfigFile, List[Tuple[str, str]]]):
        self.cluster_handler.verify_nm_configs(dic)

    def restart_services(self):
        self.cluster_handler.restart_roles()

    def setup_ssl(self, tc):
        # As a first step, set up SSL + Keystore - Required for testcase 'shuffle_ssl_enabled', for example
        if not tc.ssl_based:
            LOG.info("Testcase '%s' is not SSL-based, skipping setup...", tc.name)
            return
        if self.config.ssl_setup_mode == SSLSetupMode.SKIP:
            LOG.warning("SSL setup mode set to %s, skipping setup...", self.config.ssl_setup_mode)
            return
        elif self.config.ssl_setup_mode == SSLSetupMode.ONCE and self.context.ssl_setup_completed:
            LOG.warning("SSL setup mode set to %s, and SSL setup was completed before, skipping setup again...", self.config.ssl_setup_mode)
            return
        elif self.config.ssl_setup_mode in (SSLSetupMode.ONCE, SSLSetupMode.BETWEEN_EACH_TESTCASE):
            # Setup SSL
            self.cluster_handler.setup_ssl(self.actual_configs)
            self.context.ssl_setup_completed = True


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

    def __init__(self, cluster_handler, workdir):
        self.cluster_handler = cluster_handler
        self.workdir = workdir

    def load_configs(self, conf_file_type, conf_dict, selector, allow_empty: bool = False,
                     delete_configs: Dict[str, str] = None):
        default_config = HadoopConfigBase.create(conf_file_type)
        for k, v in conf_dict.items():
            if isinstance(v, int):
                v = str(v)
            default_config.extend_with_args({k: v})
        if delete_configs:
            default_config.remove_confs(delete_configs)

        self.cluster_handler.update_config(selector, default_config, no_backup=True, workdir=self.workdir,
                                           allow_empty=allow_empty)

    def load_properties_configs(self, conf_file_type, conf_dict, selector, allow_empty: bool = False):
        allowed_config_file_types = [HadoopConfigFile.LOG4J_PROPERTIES]
        if conf_file_type not in allowed_config_file_types:
            raise HadesException(
                "Config file type '{}' is not in allowed types: {}".format(conf_file_type, allowed_config_file_types))

        default_config = HadoopPropertiesConfig(conf_file_type)
        for k, v in conf_dict.items():
            if isinstance(v, int):
                v = str(v)
            default_config.extend_with_args({k: v})
        # TODO ???
        self.cluster_handler.update_config(selector, default_config, no_backup=True, workdir=self.workdir,
                                           allow_empty=allow_empty)


class ClusterHandler:
    def __init__(self, cluster):
        self.cluster = cluster

    def setup_ssl(self, configs: ActualConfigs):
        self.cluster.setup_ssl(NODEMANAGER_SELECTOR, configs)

    def run_app(self, app: 'ApplicationCommand', app_log_lines, selector: str = "") -> Tuple[RunnableCommand or None, TestcaseResultType]:
        app_command = self.cluster.run_app(app, selector)
        LOG.debug("Running app command '%s' in async mode on host '%s'", app_command.cmd, app_command.target.host)
        try:
            app_command.run_async(block=True, stderr=lambda line: app_log_lines.append(line),
                                  timeout=app.get_timeout_seconds())
        except HadesCommandTimedOutException:
            LOG.exception("Failed to run app command '%s'. Command '%s' timed out after %d seconds",
                          app_command.cmd, app_command, app.get_timeout_seconds())
            return app_command, TestcaseResultType.TIMEOUT
        except CommandExecutionException:
            LOG.exception("Failed to run app command '%s'. Command '%s' failed.", app_command.cmd, app_command)
            return app_command, TestcaseResultType.FAILED

        return app_command, TestcaseResultType.PASSED

    def get_log_levels(self, selector: str, packages: List[str]) -> Dict[str, List[RunnableCommand]]:
        return self.cluster.get_log_levels(selector, packages)

    def update_config(self, selector: str, config: HadoopConfigBase, no_backup: bool = False, workdir: str = ".",
                      allow_empty: bool = False):
        self.cluster.update_config(selector, config, no_backup, workdir, allow_empty)

    def get_state_and_health_report(self):
        return self.cluster.get_state_and_health_report()

    def get_config(self, selector, conf_type):
        return self.cluster.get_config(selector, conf_type)

    def save_app_logs_from_cluster(self, app_id, current_tc_dir, failed_to_compress_callback_fun):
        LOG.info("Saving application logs from cluster...")
        if app_id == APP_ID_NOT_AVAILABLE:
            return

        try:
            cmds = self.cluster.compress_and_download_app_logs(NODEMANAGER_SELECTOR, app_id,
                                                               workdir=current_tc_dir,
                                                               compress_dir=True)
            files = []
            for cmd in cmds:
                cmd.run()
                files.append(cmd.dest)
            return files
        except HadesException as he:
            LOG.exception("Error while creating targz files of application logs!")
            if "Failed to compress app logs" in str(he):
                self.save_yarn_daemon_logs(current_tc_dir, failed_to_compress_callback_fun)
                return []
            else:
                raise he

    def save_yarn_daemon_logs(self, current_tc_dir, callback):
        nm_cmds = self.cluster.compress_and_download_daemon_logs(NODEMANAGER_SELECTOR,
                                                                 workdir=current_tc_dir)
        rm_cmds = self.cluster.compress_and_download_daemon_logs(RESOURCEMANAGER_SELECTOR,
                                                                 workdir=current_tc_dir)
        cmds = nm_cmds + rm_cmds
        files = []
        for cmd in cmds:
            cmd.run()
            files.append(cmd.dest)
        callback(files)

    def read_logs_into_dict(self, logs_by_roles: LogsByRoles):
        LOG.debug("Reading YARN logs from cluster...")

        logs_by_roles.roles = self.cluster.select_roles(logs_by_roles.selector)
        logs_by_roles.log_commands = self.cluster.read_logs(follow=True, selector=logs_by_roles.selector)
        LOG.debug("YARN log commands: %s", logs_by_roles.log_commands)

        for read_logs_command in logs_by_roles.log_commands:
            LOG.debug("Running command '%s' in async mode on host '%s'", read_logs_command.cmd,
                      read_logs_command.target.host)
            read_logs_command.run_async(stdout=_callback(read_logs_command, logs_by_roles.log_lines_dict),
                                        stderr=_callback(read_logs_command, logs_by_roles.log_lines_dict))

    def restart_rm(self, sleep_after: int = 0):
        self.cluster.force_restart_roles(RESOURCEMANAGER_SELECTOR, sleep_after=sleep_after)

    def restart_nms(self, logs_by_roles):
        self.read_logs_into_dict(logs_by_roles)
        selector = NODEMANAGER_SELECTOR
        self.cluster.restart_with_guarantee(selector)

    def restart_roles(self):
        self.cluster.restart_with_guarantee(YARN_SELECTOR)

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

    def verify_nm_configs(self, dic: Dict[HadoopConfigFile, List[Tuple[str, str]]]):
        configs_per_nm = self.cluster.get_config_from_api(NODEMANAGER_SELECTOR)
        for config_file, expected_conf_list in dic.items():
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

    def set_log_levels_via_daemonlog(self, log_level_specs: List[LogLevelSpec]):
        set_log_level_cmds = []
        for spec in log_level_specs:
            set_log_level_cmds.append(
                self.cluster.set_log_level(
                    selector=spec.selector,
                    package=spec.package,
                    log_level=spec.log_level)
            )
        LOG.debug("YARN set log level commands: %s", set_log_level_cmds)

        if len(log_level_specs) != len(set_log_level_cmds):
            raise HadesException("Not all 'set loglevel' commands were created!")

        for cmd in set_log_level_cmds:
            LOG.debug("Running command '%s' in async mode on host '%s'", cmd.cmd, cmd.target.host)
            cmd.run()


class Netty4RegressionTestDriver(HadesScriptBase):
    def __init__(self, cluster: HadoopCluster, workdir: str, session_dir: str):
        super().__init__(cluster, workdir, session_dir)
        self.config = Netty4TestConfig()
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
            expected_changed_modules = True if context.patch_file else False
            self.steps.compile(expect_changed_modules=expected_changed_modules)
            self.steps.load_default_yarn_site_configs()

            for idx, tc in enumerate(testcases):
                exec_state = self.steps.init_testcase(tc)  # 1. Update context, TestResults
                if exec_state == ExecutionState.HALTED:
                    LOG.warning("Stopping test driver execution as execution state is HALTED!")
                    break

                self.steps.setup_ssl(tc)
                self.steps.restart_services()
                self.steps.load_default_configs()  # 2. Load default configs / Write initial config files
                self.steps.apply_testcase_configs()  # 3. Apply testcase configs
                self.steps.set_log_levels(permanent=True)  # 5. Set log level of ShuffleHandler to DEBUG
                self.steps.restart_services_and_save_logs(sleep_after=self.config.sleep_after_service_restart)  # 4. Restart NMs, Save logs
                self.steps.verify_log_levels([(PACKAGE_SHUFFLEHANDLER, self.config.shufflehandler_log_level),
                                              (PACKAGE_SECURITY_SSL, HadoopLogLevel.DEBUG)])
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
            # TODO Only keep latest daemonlog (for last testcase)
        self.steps.compare_results()
