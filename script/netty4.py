import gzip
import json
import os
import shutil
from typing import Callable, List
from core.cmd import RunnableCommand
from hadoop.app.example import MapReduceApp
from hadoop.config import HadoopConfig
from hadoop.xml_config import HadoopConfigFile
from script.base import HadesScriptBase
NODE_TO_RUN_ON = "type=Yarn/name=nodemanager2"
MAPREDUCE_PREFIX = "mapreduce"
MAPREDUCE_SHUFFLE_PREFIX = MAPREDUCE_PREFIX + ".shuffle"
SHUFFLE_MAX_CONNECTIONS = MAPREDUCE_SHUFFLE_PREFIX + ".max.connections"
SHUFFLE_MAX_THREADS = MAPREDUCE_SHUFFLE_PREFIX + "max.threads"
LOG_FILE_NAME_FORMAT = "{key}_{value}_{app}.log"
YARN_LOG_FORMAT = "{name} - {log}"
CONF_FORMAT = "{host}_{conf}_{key}_{value}.xml"


def _callback(host: str, logs: List[str]) -> Callable:
    def _cb(line: str):
        logs.append(YARN_LOG_FORMAT.format(name=host, log=line))
    return _cb


class Netty4RegressionTest(HadesScriptBase):
    CONFIGS = {
        SHUFFLE_MAX_CONNECTIONS: ["10", "20"],
        SHUFFLE_MAX_THREADS: ["50", "100"]
    }
    APP = MapReduceApp()

    def run(self):
        config = HadoopConfig(HadoopConfigFile.MAPRED_SITE)
        for k, v in self.CONFIGS.items():
            for config_val in v:
                files_to_compress = []
                config.extend_with_args({k: config_val})
                self.cluster.update_config("Yarn/NodeManager", config, no_backup=True)
                with self.overwrite_config(cmd_prefix="sudo -u systest"):
                    c = self.cluster.run_app(self.APP, selector="Yarn/NodeManager")
                key_name = k.replace(".", "_")
                app_log = []
                yarn_log = []
                log_commands = self.cluster.read_logs(follow=True, selector="Yarn")
                for command in log_commands:
                    command.run_async(stdout=_callback(command.target.host, yarn_log),
                                      stderr=_callback(command.target.host, yarn_log))
                c.run_async(block=True, stderr=lambda l: app_log.append(l))
                app_log_file = LOG_FILE_NAME_FORMAT.format(key=key_name, value=config_val, app="MRPI")
                with open(app_log_file, 'w') as f:
                    f.writelines(app_log)
                files_to_compress.append(app_log_file)
                yarn_log_file = LOG_FILE_NAME_FORMAT.format(key=key_name, value=config_val, app="YARN")
                with open(yarn_log_file, 'w') as f:
                    f.writelines(yarn_log)
                files_to_compress.append(yarn_log_file)
                configs = self.cluster.get_config("Yarn/NodeManager", HadoopConfigFile.MAPRED_SITE)
                for host, conf in configs.items():
                    config_file = CONF_FORMAT.format(host=host, conf=HadoopConfigFile.MAPRED_SITE.name, key=key_name,
                                                       value=config_val)
                    with open(config_file, 'w') as f:
                        f.write(conf.to_str())
                    files_to_compress.append(config_file)
                self._compress_files("{}_{}".format(key_name, config_val), files_to_compress)

    def _compress_files(self, id: str, files: List[str]):
        cmd = RunnableCommand("tar -cvf {id}.tar {files}".format(id=id, files=" ".join(files)))
        cmd.run()
        for file in files:
            os.remove(file)