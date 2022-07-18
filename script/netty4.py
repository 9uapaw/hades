import itertools
import os
from dataclasses import dataclass
from typing import Callable, List, Dict
from core.cmd import RunnableCommand
from hadoop.app.example import MapReduceApp, ApplicationCommand
from hadoop.config import HadoopConfig
from hadoop.xml_config import HadoopConfigFile
from script.base import HadesScriptBase

import logging
LOG = logging.getLogger(__name__)

NODEMANAGER_SELECTOR = "Yarn/NodeManager"
NODE_TO_RUN_ON = "type=Yarn/name=nodemanager2"
MAPREDUCE_PREFIX = "mapreduce"
YARN_APP_MAPREDUCE_PREFIX = "yarn.app.mapreduce"
YARN_APP_MAPREDUCE_SHUFFLE_PREFIX = "yarn.app.mapreduce.shuffle"
MAPREDUCE_SHUFFLE_PREFIX = MAPREDUCE_PREFIX + ".shuffle"

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


LOG_FILE_NAME_FORMAT = "{key}_{value}_{app}.log"
YARN_LOG_FORMAT = "{name} - {log}"
CONF_FORMAT = "{host}_{conf}_{key}_{value}.xml"


def _callback(host: str, logs: List[str]) -> Callable:
    def _cb(line: str):
        logs.append(YARN_LOG_FORMAT.format(name=host, log=line))

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


class Netty4RegressionTest(HadesScriptBase):
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
        SHUFFLE_LOG_BACKUPS: SHUFFLE_LOG_BACKUPS_DEFAULT
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
    APP = MapReduceApp()

    def run(self):
        # TODO Create function that prints the full yarn-site.xml / mapred-site.xml config (or saves it to the tar.gz file)?
        for tc in Netty4RegressionTest.TESTCASES:
            self._load_default_config()
            config = HadoopConfig(HadoopConfigFile.MAPRED_SITE)

            LOG.info("Running testcase: %s", tc)
            for config_key, config_val in tc.config_changes.items():
                config.extend_with_args({config_key: config_val})
                self.cluster.update_config(NODEMANAGER_SELECTOR, config, no_backup=True)
                key_name = config_key.replace(".", "_")

                yarn_log_file = self._read_logs_and_write_to_files("Yarn", key_name, config_val)
                app_log_file = self.run_app_and_collect_logs_to_file(self.APP, key_name, config_val)
                config_files = self.write_config_files(NODEMANAGER_SELECTOR, HadoopConfigFile.MAPRED_SITE, key_name, config_val)
                files_to_compress = [app_log_file, yarn_log_file] + config_files
                self._compress_files("{}_{}".format(key_name, config_val), files_to_compress)

    def _read_logs_and_write_to_files(self, selector, key_name, config_val):
        yarn_log = []
        log_commands = self.cluster.read_logs(follow=True, selector=selector)
        for read_logs_command in log_commands:
            read_logs_command.run_async(stdout=_callback(read_logs_command.target.host, yarn_log),
                                        stderr=_callback(read_logs_command.target.host, yarn_log))
        yarn_log_file = self.write_yarn_logs(yarn_log, key_name, config_val)
        return yarn_log_file

    def write_config_files(self, selector: str, conf_type: HadoopConfigFile, key_name: str, config_val: str) -> List[str]:
        configs = self.cluster.get_config(selector, conf_type)

        generated_config_files = []
        for host, conf in configs.items():
            config_file_name = CONF_FORMAT.format(host=host, conf=conf_type.name, key=key_name, value=config_val)
            with open(config_file_name, 'w') as f:
                f.write(conf.to_str())
            generated_config_files.append(config_file_name)
        return generated_config_files

    def run_app_and_collect_logs_to_file(self, app: ApplicationCommand, conf_key: str, conf_val: str) -> str:
        app_log = []
        with self.overwrite_config(cmd_prefix="sudo -u systest"):
            app_command = self.cluster.run_app(app, selector=NODEMANAGER_SELECTOR)

        app_command.run_async(block=True, stderr=lambda line: app_log.append(line))

        app_log_file = LOG_FILE_NAME_FORMAT.format(key=conf_key, value=conf_val, app="MRPI")
        with open(app_log_file, 'w') as f:
            f.writelines(app_log)
        return app_log_file

    @staticmethod
    def write_yarn_logs(log_lines_list: List[str], conf_key: str, conf_val: str):
        yarn_log_file = LOG_FILE_NAME_FORMAT.format(key=conf_key, value=conf_val, app="YARN")
        with open(yarn_log_file, 'w') as f:
            f.writelines(log_lines_list)
        return yarn_log_file

    @staticmethod
    def _compress_files(id: str, files: List[str]):
        cmd = RunnableCommand("tar -cvf {id}.tar {files}".format(id=id, files=" ".join(files)))
        cmd.run()
        for file in files:
            os.remove(file)

    def _load_default_config(self):
        LOG.info("Loading default ShuffleHandler config...")
        default_config = HadoopConfig(HadoopConfigFile.MAPRED_SITE)
        for k, v in self.DEFAULT_CONFIGS.items():
            default_config.extend_with_args({k: v})
        self.cluster.update_config(NODEMANAGER_SELECTOR, default_config, no_backup=True)
        # TODO Verify if cluster restarts / NM restarts?



