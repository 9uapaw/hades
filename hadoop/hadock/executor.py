import logging
import time
import xml.etree.ElementTree as ET
import os
from typing import List
from xml.etree.ElementTree import ElementTree, Element

import yaml

from core.cmd import RunnableCommand
from core.config import ClusterConfig, ClusterRoleConfig, ClusterContextConfig
from hadoop.app.example import ApplicationCommand
from hadoop.cluster_type import ClusterType
from hadoop.data.status import HadoopClusterStatusEntry
from hadoop.executor import HadoopOperationExecutor
from hadoop.role import HadoopRoleType, HadoopRoleInstance
from hadoop.xml_config import HadoopConfigFile

logger = logging.getLogger(__name__)


class HadockExecutor(HadoopOperationExecutor):
    HDFS_SERVICES = ["namenode", "datanode"]
    DEFAULT_COMPOSE = "docker-compose.yml"
    STATUS_COLUMN = 4
    NAME_COLUMN = -1

    def __init__(self, hadock_repository: str, hadock_compose: str = None):
        self._hadock_repository = hadock_repository
        self._hadock_compose = hadock_compose or self.DEFAULT_COMPOSE

    def discover(self) -> ClusterConfig:
        cluster = ClusterConfig(ClusterType.HADOCK)
        yarn_roles = {}
        hdfs_roles = {}

        with open("{}/{}".format(self._hadock_repository, self._hadock_compose), 'r') as f:
            compose = yaml.safe_load(f)
            for role_name, role in compose["services"].items(): # type: str, dict
                role_type = ''.join([i for i in role_name if not i.isdigit()])
                role_conf = ClusterRoleConfig()
                role_conf.type = HadoopRoleType(role_type)
                role_conf.host = role.get('container_name', role_name)
                if role_type in self.HDFS_SERVICES:
                    hdfs_roles[role_name] = role_conf
                else:
                    yarn_roles[role_name] = role_conf

        cluster.context['Yarn'] = ClusterContextConfig()
        cluster.context['Hdfs'] = ClusterContextConfig()
        cluster.context['Yarn'].name = 'Yarn'
        cluster.context['Yarn'].roles = yarn_roles
        cluster.context['Hdfs'].name = 'Hdfs'
        cluster.context['Hdfs'].roles = hdfs_roles

        return cluster

    def read_log(self, *args: HadoopRoleInstance, follow: bool = False, tail: int or None = 10) -> List[RunnableCommand]:
        cmds = []
        for role in args:
            cmd = "docker logs {} {} {}".format(role.name,
                                                "-f" if follow else "",
                                                "--tail {}".format(tail) if tail else "")
            cmds.append(RunnableCommand(cmd, target=role))

        return cmds

    def get_cluster_status(self, cluster_name: str = None) -> List[HadoopClusterStatusEntry]:
        cmd = RunnableCommand("docker container list | grep bde2020/hadoop")
        stdout, stderr = cmd.run()

        status = []
        for line in stdout:
            col = list(filter(bool, line.split("   ")))
            status.append(HadoopClusterStatusEntry(col[self.NAME_COLUMN], col[self.STATUS_COLUMN]))

        return status

    def run_app(self, random_selected: HadoopRoleInstance, application: ApplicationCommand):
        logger.info("Running app {}".format(application.__class__.__name__))

        application.path = "/opt/hadoop/share/hadoop"
        cmd = RunnableCommand("docker exec {} bash -c '{}'".format(random_selected.host, application.build()))
        cmd.run_async()

    def update_config(self, *args: HadoopRoleInstance, file: HadoopConfigFile,
                      properties: List[str], values: List[str], no_backup: bool = False, source: str = None):
        config_name = file.value.split(".")[0]
        config_ext = file.value.split(".")[1]
        all_properties = properties.copy()
        all_values = values.copy()

        if source:
            source_xml = ET.parse(source)
            root: Element = source_xml.getroot()

            for prop in root.findall('property'):  # type: Element
                prop_name = prop[0].text
                prop_value = prop.findall('value')[0].text
                all_properties.append(prop_name)
                all_values.append(prop_value)

        for role in args:
            logger.info("Setting config {} on {}".format(file.value, role.get_colorized_output()))
            local_file = "{config}-{container}-{time}.{ext}".format(
                container=role.host, config=config_name, time=int(time.time()), ext=config_ext)
            logger.info("Copying config file {} from {} as {}".format(file.value, role.host, local_file))
            xml_cmd = RunnableCommand("docker cp {container}:/etc/hadoop/{config}.{ext} {local}".format(
                container=role.host, config=config_name, time=time.time(), ext=config_ext, local=local_file
            ))
            xml_cmd.run()
            xml_file = ET.parse(local_file)
            root: Element = xml_file.getroot()
            properties_to_set = all_properties.copy()

            for prop in root.findall('property'):  # type: Element
                prop_name = prop[0].text
                prop_value = prop.findall('value')[0].text

                if prop_name in properties_to_set:
                    value_to_change = all_values[all_properties.index(prop_name)]
                    if prop_value != value_to_change:
                        prop.findall('value')[0].text = all_values[all_properties.index(prop_name)]
                        logger.debug("Setting {} to {}".format(prop_name, prop[1].text))

                    properties_to_set.remove(prop_name)

            for remaining_prop in properties_to_set:
                new_config_prop = Element('property')
                new_config_prop_name = Element('name')
                new_config_prop_name.text = remaining_prop
                new_config_value = Element('value')
                new_config_value.text = all_values[all_properties.index(remaining_prop)]

                logger.debug("Adding new property {} with value {}".format(remaining_prop, new_config_value.text))

                new_config_prop.append(new_config_prop_name)
                new_config_prop.append(new_config_value)
                root.append(new_config_prop)

            logger.info("Uploading {}".format(file.value))
            xml_file.write(file.value)
            xml_upload_cmd = RunnableCommand("docker cp {config} {container}:/etc/hadoop/{config}".format(
                container=role.host, config=file.value))
            xml_upload_cmd.run()
            os.remove(file.value)

            if no_backup:
                logger.info("Backup is turned off. Deleting file {}".format(local_file))
                os.remove(local_file)

    def restart_roles(self, *args: HadoopRoleInstance):
        for role in args:
            logger.info("Restarting {}".format(role.get_colorized_output()))
            restart_cmd = RunnableCommand("docker restart {}".format(role.host))
            logger.info(restart_cmd.run())
