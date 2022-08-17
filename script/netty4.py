import itertools
import logging
import os.path
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, List, Dict, Tuple

from tabulate import tabulate

from core.cmd import RunnableCommand
from core.error import ScriptException, HadesCommandTimedOutException, HadesException
from core.handler import MainCommandHandler
from core.util import FileUtils, CompressedFileUtils, PrintUtils
from hadoop.app.example import MapReduceApp, ApplicationCommand, MapReduceAppType
from hadoop.cluster import HadoopCluster, HadoopLogLevel
from hadoop.config import HadoopConfig
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
TC_LIMIT_UNLIMITED = 99999999

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
                testcases.append(Netty4Testcase(self._generate_tc_name(tc_counter, app_type), conf_changes, config.mr_apps[app_type]))
        return testcases

    def _generate_tc_name(self, tc_counter, app_type: MapReduceAppType):
        return f"{self.name}_{str(tc_counter)}_{app_type.value}"


@dataclass
class Netty4Testcase:
    name: str
    config_changes: Dict[str, str]
    app: MapReduceApp

    def __hash__(self):
        return hash(self.name)


class TestcaseResultType(Enum):
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


@dataclass
class LogsByRoles:
    cluster: HadoopCluster
    selector: str
    roles = None
    log_commands = None
    written_files = None

    def __post_init__(self):
        self.log_lines_dict = {}

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

    def write_to_files(self, context, prefix: str = "", app: str = ""):
        self.written_files = []
        if not self.log_lines_dict:
            raise HadesException("YARN log lines dictionary is empty!")
        if not prefix and not app:
            raise HadesException("Either prefix or app should be specified for this method!")

        for cmd, lines in self.log_lines_dict.items():
            if prefix:
                yarn_log_file = PREFIXED_YARN_LOG_FILE_NAME_FORMAT.format(prefix=prefix,
                                                                          host=cmd.target.host,
                                                                          role=cmd.target.role_type.name)
            else:
                yarn_log_file = YARN_LOG_FILE_NAME_FORMAT.format(host=cmd.target.host,
                                                                 role=cmd.target.role_type.name,
                                                                 app="YARN")
            file_path = os.path.join(context.current_tc_dir, yarn_log_file)
            self.written_files.append(file_path)
            with open(file_path, 'w') as f:
                f.writelines(lines)

    def search_in_logs(self, verification: LogVerification):
        filtered_roles = list(filter(lambda rc: rc.target.role_type in [verification.role_type], list(self.log_lines_dict.keys())))

        valid = True if verification.inverted_mode else False
        bad_role = None
        for role in filtered_roles:
            for line in self.log_lines_dict[role]:
                if verification.text in line:
                    if verification.inverted_mode:
                        valid = False
                        bad_role = role
                        break
                    else:
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

    # Dynamic fields
    workdir = None
    tc_no = None
    tc = None
    no_of_testcases = None
    current_tc_dir = None

    def __str__(self):
        return f"Context: {self.name}"

    def __hash__(self):
        return hash(self.name)

    def update_current_tc(self, idx: int, workdir: str, tc: Netty4Testcase, no_of_testcases: int):
        self.workdir = workdir
        self.tc_no = idx + 1
        self.tc = tc
        self.no_of_testcases = no_of_testcases
        # TODO Consolidate working dir handling to here / Change working dir to ctx/tcname/... + use it from only a single place!
        self.current_tc_dir = os.path.join(workdir, tc.name)
        if not os.path.exists(self.current_tc_dir):
            os.mkdir(self.current_tc_dir)
        LOG.debug("Current TC dir is: %s", self.current_tc_dir)

    def log_running_testcase(self):
        LOG.info("[%s] [%d / %d] Running testcase: %s", self, self.tc_no, self.no_of_testcases, self.tc)
        PrintUtils.print_banner_figlet(f"STARTING TESTCASE: {self.tc.name}")

    def get_targz_filename(self):
        tc_no = f"0{str(self.tc_no)}" if self.tc_no < 9 else str(self.tc_no)
        tc_targz_filename = os.path.join(self.workdir, f"testcase_{tc_no}_{self.tc.name}.tar.gz")
        return tc_targz_filename


@dataclass
class Netty4TestConfig:
    # TODO
    LIMIT_TESTCASES = True
    QUICK_MODE = True
    testcase_limit = 1 if QUICK_MODE or LIMIT_TESTCASES else TC_LIMIT_UNLIMITED
    enable_compilation = False if QUICK_MODE else True
    allow_verification_failure = True if QUICK_MODE else False

    extract_tar_files = True
    timeout = 120
    compress_tc_result = False
    decompress_app_container_logs = True
    shufflehandler_log_level = HadoopLogLevel.DEBUG

    def __post_init__(self):
        sleep_job = MapReduceApp(MapReduceAppType.SLEEP, cmd='sleep -m 1 -r 1 -mt 10 -rt 10', timeout=self.timeout)
        pi_job = MapReduceApp(MapReduceAppType.PI, cmd='pi 1 1000', timeout=self.timeout)
        loadgen_job = MapReduceApp(MapReduceAppType.LOADGEN,
                                   cmd=f"loadgen -m 200 -r 150 -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text",
                                   timeout=300)

        sort_input_dir = "/user/systest/sortInputDir"
        sort_output_dir = "/user/systest/sortOutputDir"
        random_writer_job = MapReduceApp(MapReduceAppType.RANDOM_WRITER, cmd=f"randomwriter {sort_input_dir}")
        mapred_sort_job = MapReduceApp(MapReduceAppType.TEST_MAPRED_SORT,
                                       cmd=f"testmapredsort -sortInput {sort_input_dir} -sortOutput {sort_output_dir}")

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
                                           compile=self.enable_compilation,
                                           allow_verification_failure=self.allow_verification_failure),
                         Netty4TestContext("with netty patch based on trunk",
                                           DEFAULT_BRANCH,
                                           patch_file=patch,
                                           log_verifications=[LogVerification(HadoopRoleType.NM, netty_log_message, inverted_mode=False)],
                                           compile=self.enable_compilation,
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


class Netty4TestResults:
    def __init__(self):
        self.results: Dict[Netty4TestContext, Dict[Netty4Testcase, TestcaseResult]] = {}
        self.context = None
        self.tc = None

    @property
    def current_result(self):
        return self.results[self.context][self.tc]

    @property
    def is_current_tc_timed_out(self):
        curr = self.results[self.context][self.tc]
        return curr.type == TestcaseResultType.TIMEOUT

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
    INITIAL_CONFIG = "initial_config"
    TC_CONFIG = "testcase_config"
    APP_LOG_TAR_GZ = "app_log_tar_gz"
    ALL_FILES_TAR_GZ = "all_files_tar_gz"
    NM_RESTART_LOGS = "nm_restart_logs"
    YARN_DAEMON_LOGS = "yarn_daemon_logs"
    APP_LOG_FILE = "app_log_file"
    EXTRACTED_APP_LOG_FILES = "extracted_app_log_files"


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
        return self._curr_files_dict[out_type]

    def get_all_for_current_ctx(self):
        res = list(itertools.chain(self._curr_files_dict.values()))
        return res

    def print_all(self):
        LOG.debug("All generated files: %s", self._files)

    def print_all_for_current_ctx(self):
        res = self.get_all_for_current_ctx()
        LOG.debug("All files for context '%s' / testcase '%s': %s", self.ctx, self.tc, res)

    def verify(self):
        if not self.get(OutputFileType.TC_CONFIG):
            raise HadesException("Expected non-empty testcase config files list!")
        if not self.get(OutputFileType.INITIAL_CONFIG):
            raise HadesException("Expected non-empty initial config files list!")
        if not self.get(OutputFileType.APP_LOG_TAR_GZ):
            raise HadesException("Expected non-empty app log tar files list!")
        if not self.get(OutputFileType.YARN_DAEMON_LOGS):
            raise HadesException("Expected non-empty YARN log files list!")
        if not self.get(OutputFileType.NM_RESTART_LOGS):
            raise HadesException("Expected non-empty YARN NM restart log files!")


class Netty4RegressionTest(HadesScriptBase):
    def __init__(self, cluster: HadoopCluster, workdir: str):
        super().__init__(cluster, workdir)
        LOG.info("Using workdir: %s", self.workdir)
        self.tc = None
        self.config = Netty4TestConfig()
        self.context = None

    YARN_SITE_DEFAULT_CONFIGS = {
        CONF_DEBUG_DELAY: "99999999"
    }

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

        self.test_results = Netty4TestResults()
        for self.context in self.config.contexts:
            LOG.info("Starting: %s", self.context)

            # Checkout branch, apply patch if required
            hadoop_dir = HadoopDir(handler.ctx.config.hadoop_path)
            hadoop_dir.switch_branch_to(self.context.base_branch)

            if self.context.patch_file:
                hadoop_dir.apply_patch(self.context.patch_file, force_reset=True)

            LOG.info("[%s] Starting compilation...", self.context)
            if self.context.compile:
                handler.compile(all=True, changed=False, deploy=True, modules=None, no_copy=True, single=None)

            self._load_default_yarn_site_configs()

            self.generated_files = GeneratedOutputFiles()
            for idx, self.tc in enumerate(testcases):
                # 1. Update context, TestResults
                self.context.update_current_tc(idx, self.workdir, self.tc, no_of_testcases)
                self.test_results.update_with_context_and_testcase(self.context, self.tc)
                self.generated_files.update_with_ctx_and_tc(self.context, self.tc)
                self.context.log_running_testcase()

                # 2. Load default configs
                self._load_default_mapred_configs()
                config = HadoopConfig(HadoopConfigFile.MAPRED_SITE)

                # 3. Write initial config files
                self.generated_files.register_files(OutputFileType.INITIAL_CONFIG, self.write_config_files(self.context,
                                                                                                           NODEMANAGER_SELECTOR,
                                                                                                           HadoopConfigFile.MAPRED_SITE,
                                                                                                           dir=CONF_DIR_INITIAL)
                                                    )

                # 4. Apply testcase configs
                for config_key, config_val in self.tc.config_changes.items():
                    config.extend_with_args({config_key: config_val})

                self.cluster.update_config(NODEMANAGER_SELECTOR, config, no_backup=True, workdir=self.workdir)

                # 5. Set log level of ShuffleHandler to DEBUG
                self._set_shufflehandler_loglevel()

                # 6. Restart NodeManagers with new config + Save NodeManager logs
                nm_restart_logs = LogsByRoles(self.cluster, selector=NODEMANAGER_SELECTOR)
                self._restart_nms(logs_by_roles=nm_restart_logs)
                self.write_yarn_restart_logs(self.context, nm_restart_logs)
                self.generated_files.register_files(OutputFileType.NM_RESTART_LOGS, nm_restart_logs.written_files)

                yarn_logs = LogsByRoles(self.cluster, selector="Yarn")
                yarn_logs.read_logs_into_dict()

                # 7. Run app of the testcase + Record test results
                result, app_log_file = self.run_app_and_collect_logs_to_file(self.context, self.tc.app)
                self.generated_files.register_files(OutputFileType.APP_LOG_FILE, [app_log_file])
                self.test_results.update_with_result(self.tc, result)

                # 8. Get application according to status
                if self.test_results.is_current_tc_timed_out:
                    LOG.debug("[%s] Getting running app id as testcase timed out", self.context)
                    app_id = self._get_latest_running_app()
                else:
                    LOG.debug("[%s] Getting finished app id as testcase passed", self.context)
                    app_id = self._get_latest_finished_app()

                # 9. Ensure if version is correct by checking certain logs
                if self.context.log_verifications:
                    try:
                        for verification in self.context.log_verifications:
                            yarn_logs.search_in_logs(verification)
                    except HadesException as e:
                        if self.context.allow_verification_failure:
                            LOG.exception("Allowed verification failure!")
                        else:
                            raise e

                # 10. Write YARN daemon logs - Now it's okay to write the YARN log files as the app is either finished or timed out
                self.write_yarn_logs(self.context, yarn_logs)
                self.generated_files.register_files(OutputFileType.YARN_DAEMON_LOGS, yarn_logs.written_files)
                self.generated_files.register_files(OutputFileType.APP_LOG_TAR_GZ, self._get_app_log_tar_files(self.context, app_id))
                self.generated_files.register_files(OutputFileType.TC_CONFIG,
                                                    self.write_config_files(self.context,
                                                                            NODEMANAGER_SELECTOR,
                                                                            HadoopConfigFile.MAPRED_SITE,
                                                                            dir=CONF_DIR_TC)
                                                    )
                self.generated_files.verify()
                yarn_logs.verify_no_empty_lines()

                if self.config.decompress_app_container_logs:
                    for app_log_tar_file in self.generated_files.get(OutputFileType.APP_LOG_TAR_GZ):
                        target_dir = os.path.join(self.context.current_tc_dir)
                        LOG.debug("[%s] Extracting file '%s' to %s", self.context, app_log_tar_file, target_dir)
                        CompressedFileUtils.extract_targz_file(app_log_tar_file, target_dir)
                        self.generated_files.register_files(OutputFileType.EXTRACTED_APP_LOG_FILES,
                                                            CompressedFileUtils.list_targz_file(app_log_tar_file),
                                                            allow_multiple=True)

                if self.config.compress_tc_result:
                    tar_gz_filename = self.context.get_targz_filename()
                    if self.using_custom_workdir:
                        FileUtils.compress_dir(filename=tar_gz_filename, dir=self.context.current_tc_dir)
                    else:
                        FileUtils.compress_files(filename=tar_gz_filename, files=self.generated_files.get_all_for_current_ctx())
                    self.generated_files.register_files(OutputFileType.ALL_FILES_TAR_GZ, [tar_gz_filename])
                    FileUtils.rm_dir(self.context.current_tc_dir)
                self.generated_files.print_all_for_current_ctx()
            self.generated_files.print_all()
            self.test_results.print_report()
        self._compare_results()

    def _get_app_log_tar_files(self, context, app_id: str):
        if app_id == APP_ID_NOT_AVAILABLE:
            return []

        app_log_tar_files = []
        cmds = self.cluster.compress_and_download_app_logs(NODEMANAGER_SELECTOR, app_id, workdir=context.current_tc_dir, compress_dir=True)
        for cmd in cmds:
            cmd.run()
            app_log_tar_files.append(cmd.dest)
        return app_log_tar_files

    def _get_single_running_app(self):
        cmd = self.cluster.get_running_apps()
        running_apps, stderr = cmd.run()
        if len(running_apps) > 1:
            raise ScriptException(f"Expected 1 running application. Found more: {running_apps}")
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

    def _restart_nms(self, logs_by_roles: LogsByRoles):
        logs_by_roles.read_logs_into_dict()
        handlers = []
        for cmd in self.cluster.restart_roles(NODEMANAGER_SELECTOR):
            handlers.append(cmd.run_async())
        for h in handlers:
            h.wait()

    def _set_shufflehandler_loglevel(self):
        LOG.debug("Setting ShuffleHandler log level to: %s", self.config.shufflehandler_log_level)

        set_log_level_cmds: List[RunnableCommand] = self.cluster.set_log_level(package="org.apache.hadoop.mapred.ShuffleHandler",
                                                                               log_level=self.config.shufflehandler_log_level)
        LOG.debug("YARN set log level commands: %s", set_log_level_cmds)
        for cmd in set_log_level_cmds:
            LOG.debug("Running command '%s' in async mode on host '%s'", cmd.cmd, cmd.target.host)
            cmd.run()

    def write_config_files(self, context, selector: str, conf_type: HadoopConfigFile, dir=None) -> List[str]:
        configs = self.cluster.get_config(selector, conf_type)

        generated_config_files = []
        for host, conf in configs.items():
            config_file_name = CONF_FORMAT.format(host=host, conf=conf_type.name)
            if dir:
                dir_path = os.path.join(context.current_tc_dir, dir)
                if not os.path.exists(dir_path):
                    os.mkdir(dir_path)
                file_path = os.path.join(dir_path, config_file_name)
            else:
                file_path = os.path.join(context.current_tc_dir, config_file_name)

            LOG.debug("Writing config file '%s' on host '%s'", file_path, host)
            with open(file_path, 'w') as f:
                f.write(conf.to_str())
            generated_config_files.append(file_path)
        return generated_config_files

    def run_app_and_collect_logs_to_file(self, context, app: ApplicationCommand) -> Tuple[TestcaseResult, str]:
        app_log = []
        timed_out = False
        with self.overwrite_config(cmd_prefix="sudo -u systest"):
            app_command = self.cluster.run_app(app, selector=NODEMANAGER_SELECTOR)

        LOG.debug("Running app command '%s' in async mode on host '%s'", app_command.cmd, app_command.target.host)
        try:
            app_command.run_async(block=True, stderr=lambda line: app_log.append(line), timeout=app.get_timeout_seconds())
        except HadesCommandTimedOutException:
            timed_out = True
            LOG.error("Command '%s' timed out after %d seconds", app_command, app.get_timeout_seconds())

        app_log_file = APP_LOG_FILE_NAME_FORMAT.format(app="MRPI")
        app_log_file_path = os.path.join(context.current_tc_dir, app_log_file)
        LOG.debug("Writing app log file '%s' on host '%s'", app_log_file_path, app_command.target.host)
        with open(app_log_file_path, 'w') as f:
            f.writelines(app_log)

        if timed_out:
            return TestcaseResult(TestcaseResultType.TIMEOUT, app_command, details=TIMEOUT_MSG_TEMPLATE.format(app.get_timeout_seconds())), app_log_file_path
        return TestcaseResult(TestcaseResultType.PASSED, app_command), app_log_file_path

    @staticmethod
    def write_yarn_logs(context, logs_by_roles: LogsByRoles):
        logs_by_roles.write_to_files(context, prefix="", app="YARN")

    @staticmethod
    def write_yarn_restart_logs(context, logs_by_roles: LogsByRoles):
        logs_by_roles.write_to_files(context, prefix="restart-", app="YARN")

    def _load_default_mapred_configs(self):
        LOG.info("Loading default MR ShuffleHandler configs...")
        self._load_configs(HadoopConfigFile.MAPRED_SITE, DEFAULT_CONFIGS, NODEMANAGER_SELECTOR)

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

    def _compare_results(self):
        if self.config.testcase_limit < TC_LIMIT_UNLIMITED:
            LOG.warning("Skipping comparison of testcase results as the testcase limit is set to: %s", self.config.testcase_limit)
            return
        self.test_results.compare(self.config.contexts[0])
