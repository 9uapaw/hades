import itertools
import os.path
from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Dict
from core.cmd import RunnableCommand
from core.error import ScriptException, HadesCommandTimedOutException
from core.util import FileUtils
from hadoop.app.example import MapReduceApp, ApplicationCommand
from hadoop.cluster import HadoopCluster
from hadoop.config import HadoopConfig
from hadoop.xml_config import HadoopConfigFile
from script.base import HadesScriptBase
from tabulate import tabulate

import logging

CONF_DIR_TC = "testcase_config"

CONF_DIR_INITIAL = "initial_config"

APP_ID_NOT_AVAILABLE = "N/A"

LOG = logging.getLogger(__name__)

NODEMANAGER_SELECTOR = "Yarn/NodeManager"
NODE_TO_RUN_ON = "type=Yarn/name=nodemanager2"
MAPREDUCE_PREFIX = "mapreduce"
YARN_APP_MAPREDUCE_PREFIX = "yarn.app.mapreduce"
YARN_APP_MAPREDUCE_SHUFFLE_PREFIX = "yarn.app.mapreduce.shuffle"
MAPREDUCE_SHUFFLE_PREFIX = MAPREDUCE_PREFIX + ".shuffle"

CONF_DEBUG_DELAY = "yarn.nodemanager.delete.debug-delay-sec"

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


APP_LOG_FILE_NAME_FORMAT = "app_{app}.log"
YARN_LOG_FILE_NAME_FORMAT = "{host}_{role}_{app}.log"
YARN_LOG_FORMAT = "{name} - {log}"
CONF_FORMAT = "{host}_{conf}.xml"
DEFAULT_TIMEOUT = 30
TIMEOUT_MSG = "Timed out after {} seconds".format(DEFAULT_TIMEOUT)


def _callback(cmd: RunnableCommand, logs_dict: Dict[RunnableCommand, List[str]]) -> Callable:
    def _cb(line: str):
        if cmd not in logs_dict or not logs_dict[cmd]:
            logs_dict[cmd] = []
        logs_dict[cmd].append(YARN_LOG_FORMAT.format(name=cmd.target.host, log=line))

    return _cb


class Netty4TestcasesBuilder:
    def __init__(self, name):
        self.configs: Dict[str, List[str]] = {}
        self.name = name

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

    def generate_testcases(self):
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
            testcases.append(Netty4Testcase(self._generate_tc_name(tc_counter), conf_changes))
        return testcases

    def _generate_tc_name(self, tc_counter):
        return self.name + '_' + str(tc_counter)


@dataclass
class Netty4Testcase:
    name: str
    config_changes: Dict[str, str]

    def __hash__(self):
        return hash(self.name)


class TestcaseResultType(Enum):
    TIMEOUT = "timed out"
    PASSED = "passed"


@dataclass
class TestcaseResult:
    type: TestcaseResultType
    app_command: RunnableCommand
    app_log_file: str
    details: str = None


class Netty4RegressionTest(HadesScriptBase):
    def __init__(self, cluster: HadoopCluster, workdir: str):
        super().__init__(cluster, workdir)
        LOG.info("Using workdir: %s", self.workdir)
        self.tc = None
        self.current_tc_dir = None

    TC_LIMIT = 1

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

    YARN_SITE_DEFAULT_CONFIGS = {
        CONF_DEBUG_DELAY: "99999999"
    }

    TESTCASES = [
        *Netty4TestcasesBuilder("shuffle_max_connections")
            .with_configs(SHUFFLE_MAX_CONNECTIONS, ["2", "5"])
            .generate_testcases(),
        *Netty4TestcasesBuilder("shuffle_max_threads")
            .with_configs(SHUFFLE_MAX_THREADS, ["3", "6"])
            .generate_testcases(),
        *Netty4TestcasesBuilder("shuffle_max_open_files")
            .with_configs(SHUFFLE_MAX_SESSION_OPEN_FILES, ["2", "5"])
            .generate_testcases(),
        *Netty4TestcasesBuilder("shuffle_listen_queue_size")
            .with_configs(SHUFFLE_LISTEN_QUEUE_SIZE, ["10", "50"])
            .generate_testcases(),
        *Netty4TestcasesBuilder("shuffle_ssl_enabled")
            .with_configs(SHUFFLE_SSL_ENABLED, ["true"])
            .generate_testcases(),
        *Netty4TestcasesBuilder("keepalive")
            .with_config(SHUFFLE_CONNECTION_KEEPALIVE_ENABLE, "true")
            .with_configs(SHUFFLE_CONNECTION_KEEPALIVE_TIMEOUT, ["15", "25"])
            .generate_testcases()
    ]
    APP = MapReduceApp(cmd='sleep -m 1 -r 1 -mt 10 -rt 10')

    def run(self):
        testcases = Netty4RegressionTest.TESTCASES
        if Netty4RegressionTest.TC_LIMIT > 0:
            LOG.info("Limiting testcases to %s", Netty4RegressionTest.TC_LIMIT)
            testcases = testcases[:Netty4RegressionTest.TC_LIMIT]
        no_of_tcs = len(testcases)
        LOG.info("Will run %d testcases", no_of_tcs)
        LOG.info("Testcases: %s", testcases)

        self._load_default_yarn_site_configs()

        # TODO search for ".*"\.format and replace them with f-strings
        testcase_results: Dict[Netty4Testcase, TestcaseResult] = {}
        for idx, self.tc in enumerate(testcases):
            self.current_tc_dir = os.path.join(self.workdir, self.tc.name)
            os.mkdir(self.current_tc_dir)
            LOG.debug("Current TC dir is: %s", self.current_tc_dir)

            self._load_default_mapred_configs()
            config = HadoopConfig(HadoopConfigFile.MAPRED_SITE)
            initial_config_files: List[str] = self.write_config_files(NODEMANAGER_SELECTOR,
                                                                      HadoopConfigFile.MAPRED_SITE, dir=CONF_DIR_INITIAL)
            LOG.info("[%d\\%d] Running testcase: %s", idx + 1, no_of_tcs, self.tc)
            for config_key, config_val in self.tc.config_changes.items():
                config.extend_with_args({config_key: config_val})

            self.cluster.update_config(NODEMANAGER_SELECTOR, config, no_backup=True, workdir=self.workdir)
            self._restart_nms()

            yarn_log_files: List[str] = self._read_logs_and_write_to_files("Yarn")
            testcase_results[self.tc] = self.run_app_and_collect_logs_to_file(self.APP)

            if testcase_results[self.tc].type == TestcaseResultType.TIMEOUT:
                LOG.debug("Getting running app id as testcase timed out")
                app_id = self._get_latest_running_app()
            else:
                LOG.debug("Getting finished app id as testcase passed")
                app_id = self._get_latest_finished_app()
            app_log_tar_files = self._get_app_log_tar_files(app_id)
            tc_config_files: List[str] = self.write_config_files(NODEMANAGER_SELECTOR, HadoopConfigFile.MAPRED_SITE,
                                                                 dir=CONF_DIR_TC)
            files_to_compress = [testcase_results[self.tc].app_log_file] + \
                                yarn_log_files + \
                                tc_config_files + \
                                initial_config_files + \
                                app_log_tar_files
            tc_no = f"0_{str(idx + 1)}" if idx < 9 else str(idx + 1)
            tc_targz_filename = os.path.join(self.workdir, f"testcase_{tc_no}_{self.tc.name}.tar.gz")

            if self.using_custom_workdir:
                FileUtils.compress_dir(filename=tc_targz_filename, dir=self.current_tc_dir)
            else:
                FileUtils.compress_files(filename=tc_targz_filename, files=files_to_compress)
            FileUtils.rm_dir(self.current_tc_dir)

        self._print_report(testcase_results)

    def _get_app_log_tar_files(self, app_id: str):
        if app_id == APP_ID_NOT_AVAILABLE:
            return []

        app_log_tar_files = []
        cmds = self.cluster.compress_and_download_app_logs(NODEMANAGER_SELECTOR, app_id, workdir=self.current_tc_dir)
        for cmd in cmds:
            cmd.run()
            app_log_tar_files.append(cmd.dest)
        return app_log_tar_files

    def _get_single_running_app(self):
        cmd = self.cluster.get_running_apps()
        running_apps, stderr = cmd.run()
        if len(running_apps) > 1:
            raise ScriptException("Expected 1 running application. Found more: {}".format(running_apps))
        elif len(running_apps) == 0:
            raise ScriptException("Expected 1 running application. Found no application")
        current_app_id = running_apps[0]
        LOG.info("Found running application: %s", current_app_id)
        return current_app_id

    def _get_latest_running_app(self):
        cmd = self.cluster.get_running_apps()
        running_apps, stderr = cmd.run()
        if len(running_apps) == 0:
            raise ScriptException("Expected 1 running application. Found no application")
        current_app_id = running_apps[0]
        LOG.info("Found running application: %s", current_app_id)
        return current_app_id

    def _get_latest_finished_app(self):
        cmd = self.cluster.get_finished_apps()
        finished_apps, stderr = cmd.run()
        LOG.info("Found finished applications: %s", finished_apps)
        # Topmost row is the latest app
        return finished_apps[0]

    def _restart_nms(self):
        handlers = []
        for cmd in self.cluster.restart_roles(NODEMANAGER_SELECTOR):
            handlers.append(cmd.run_async())
        for h in handlers:
            h.wait()

    def _read_logs_and_write_to_files(self, selector):
        LOG.debug("Reading YARN logs from cluster...")
        yarn_log_lines = {}
        log_commands: List[RunnableCommand] = self.cluster.read_logs(follow=True, selector=selector)
        LOG.debug("YARN log commands: %s", log_commands)
        for read_logs_command in log_commands:
            LOG.debug("Running command '%s' in async mode on host '%s'", read_logs_command.cmd,
                      read_logs_command.target.host)
            read_logs_command.run_async(stdout=_callback(read_logs_command, yarn_log_lines),
                                        stderr=_callback(read_logs_command, yarn_log_lines))
        return self.write_yarn_logs(yarn_log_lines)

    def write_config_files(self, selector: str, conf_type: HadoopConfigFile, dir=None) -> List[
        str]:
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

    def run_app_and_collect_logs_to_file(self, app: ApplicationCommand) -> TestcaseResult:
        app_log = []
        timeout = False
        with self.overwrite_config(cmd_prefix="sudo -u systest"):
            app_command = self.cluster.run_app(app, selector=NODEMANAGER_SELECTOR)

        LOG.debug("Running app command '%s' in async mode on host '%s'", app_command.cmd, app_command.target.host)
        try:
            app_command.run_async(block=True, stderr=lambda line: app_log.append(line), timeout=DEFAULT_TIMEOUT)
        except HadesCommandTimedOutException:
            timeout = True
            LOG.error("Command timed out: %s", app_command)

        app_log_file = APP_LOG_FILE_NAME_FORMAT.format(app="MRPI")
        file_path = os.path.join(self.current_tc_dir, app_log_file)
        LOG.debug("Writing app log file '%s' on host '%s'", file_path, app_command.target.host)
        with open(file_path, 'w') as f:
            f.writelines(app_log)

        if timeout:
            return TestcaseResult(TestcaseResultType.TIMEOUT, app_command, file_path, details=TIMEOUT_MSG)
        return TestcaseResult(TestcaseResultType.PASSED, app_command, file_path)

    def write_yarn_logs(self, log_lines_dict: Dict[RunnableCommand, List[str]]):
        files = []
        for cmd, lines in log_lines_dict.items():
            yarn_log_file = YARN_LOG_FILE_NAME_FORMAT.format(host=cmd.target.host,
                                                             role=cmd.target.role_type.name, app="YARN")
            file_path = os.path.join(self.current_tc_dir, yarn_log_file)
            files.append(file_path)
            with open(file_path, 'w') as f:
                f.writelines(lines)
        return files

    def _load_default_mapred_configs(self):
        LOG.info("Loading default MR ShuffleHandler configs...")
        self._load_configs(HadoopConfigFile.MAPRED_SITE, self.DEFAULT_CONFIGS, NODEMANAGER_SELECTOR)

    def _load_default_yarn_site_configs(self):
        LOG.info("Loading default yarn-site.xml configs...")
        self._load_configs(HadoopConfigFile.YARN_SITE, self.YARN_SITE_DEFAULT_CONFIGS, NODEMANAGER_SELECTOR)

    def _load_configs(self, conf_file_type, conf_dict, selector, ):
        default_config = HadoopConfig(conf_file_type)
        for k, v in conf_dict.items():
            if isinstance(v, int):
                v = str(v)
            default_config.extend_with_args({k: v})
        self.cluster.update_config(selector, default_config, no_backup=True, workdir=self.workdir)

    @staticmethod
    def _print_report(testcase_results: Dict[Netty4Testcase, TestcaseResult]):
        sep = "=" * 60
        LOG.info(sep)
        LOG.info("TESTCASE RESULTS")
        LOG.info(sep)

        data = []
        for tc, result in testcase_results.items():
            data.append([tc.name, result.app_command.cmd, result.type.value, result.details])
        tabulated = tabulate(data, ["TESTCASE", "COMMAND", "RESULT", "DETAILS"], tablefmt="fancy_grid")
        LOG.info("\n" + tabulated)
