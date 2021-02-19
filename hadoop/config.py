import logging
from typing import Dict

from core.error import HadesException
from hadoop.xml_config import HadoopConfigFile
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree, Element


logger = logging.getLogger(__name__)


class HadoopConfig:

    def __init__(self, file: HadoopConfigFile, base: str = None):
        self._file = file
        self._extension: Dict[str, str] = {}
        if base:
            self._base_xml = ET.parse(base)
        else:
            self._base_xml = None

    @property
    def file(self) -> str:
        return str(self._file.value)

    @property
    def xml(self) -> ElementTree:
        return self._base_xml

    @xml.setter
    def xml(self, path: str):
        self._base_xml = ET.parse(path)

    def extend_with_xml(self, path: str):
        source_xml = ET.parse(path)
        root: Element = source_xml.getroot()

        for prop in root.findall('property'):  # type: Element
            prop_name = prop[0].text
            prop_value = prop.findall('value')[0].text
            if prop_name not in self._extension:
                self._extension[prop_name] = prop_value

    def extend_with_args(self, args: Dict[str, str]):
        self._extension.update(args)

    def merge(self):
        if not self._base_xml:
            raise HadesException("Can not merge without base xml. Set base xml before calling merge.")

        properties_to_set = set(self._extension.keys())
        for prop in self._base_xml.getroot().findall('property'):  # type: Element
            prop_name = prop[0].text
            prop_value = prop.findall('value')[0].text

            if prop_name in self._extension:
                if prop_value != self._extension[prop_name]:
                    prop.findall('value')[0].text = self._extension[prop_name]
                    logger.debug("Setting {} to {}".format(prop_name, prop[1].text))

                properties_to_set.remove(prop_name)

        for remaining_prop in properties_to_set:
            new_config_prop = Element('property')
            new_config_prop_name = Element('name')
            new_config_prop_name.text = remaining_prop
            new_config_value = Element('value')
            new_config_value.text = self._extension[remaining_prop]

            logger.debug("Adding new property {} with value {}".format(remaining_prop, new_config_value.text))

            new_config_prop.append(new_config_prop_name)
            new_config_prop.append(new_config_value)
            self._base_xml.getroot().append(new_config_prop)

    def commit(self):
        self._base_xml.write(self._file.value)