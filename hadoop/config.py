import configparser
import itertools
import logging
from abc import ABC, abstractmethod
from collections import Iterable
from typing import Dict, Iterator, Tuple, List

from core.error import HadesException
from hadoop.hadoop_config import HadoopConfigFile, HadoopConfigFileType
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ElementTree, Element

EXTENSION_SEPARATOR = "#EXTENSION\n"

logger = logging.getLogger(__name__)


class XMLConfigIterator(Iterator):

    def __init__(self, xml: ElementTree) -> None:
        self._it: Iterator[Element] = xml.findall('property').__iter__()

    def __next__(self) -> Tuple[str, str]:
        prop = next(self._it)
        prop_name = prop[0].text
        prop_value = prop[1].text

        return prop_name, prop_value


class HadoopConfigBase(ABC, Iterable):
    @abstractmethod
    def set_base_config(self, path: str):
        pass

    @abstractmethod
    def merge(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @property
    @abstractmethod
    def file(self):
        pass

    @classmethod
    def create(cls, config: HadoopConfigFile, base_path: str = None):
        if config.config_type == HadoopConfigFileType.XML:
            return HadoopXMLConfig(config, base_path)
        elif config.config_type == HadoopConfigFileType.PROPERTIES:
            return HadoopPropertiesConfig(config, base_path)
        elif config.config_type == HadoopConfigFileType.SHELL:
            return HadoopShellScriptConfig(config, base_path)


class HadoopXMLConfig(HadoopConfigBase):
    def __init__(self, file: HadoopConfigFile, base_path: str = None):
        self._file = file
        self._extension: Dict[str, str] = {}
        self._removals: Dict[str, str] = {}

        if base_path:
            self._base_xml = ET.parse(base_path)
        else:
            self._base_xml = None

    def __iter__(self) -> Iterator[Tuple[str, str]]:
        if self._base_xml:
            return XMLConfigIterator(self.xml)
        else:
            return self._extension.items().__iter__()

    @property
    def file(self) -> str:
        return str(self._file.val)

    @property
    def xml(self) -> ElementTree:
        return self._base_xml

    def set_base_config(self, path: str):
        self._base_xml = ET.parse(path)

    def set_xml_str(self, xml_str: str):
        self._base_xml = ET.XML(xml_str)

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

    def remove_confs(self, confs: Dict[str, str]):
        self._removals = confs

    def merge(self):
        if not self._base_xml:
            raise HadesException("Can not merge without base xml. Set base xml before calling merge.")

        intersection = set(self._extension.keys()).intersection(set(self._removals.keys()))
        if intersection:
            raise ValueError("Invalid config update, some keys are set to update + remove at the same time. Removals: {}, Updates: {}".format(self._removals, self._extension))

        properties_to_set = set(self._extension.keys())
        root = self._get_root()
        for prop in root.findall('property'):  # type: Element
            prop_name = prop[0].text
            prop_value = prop.findall('value')[0].text

            if prop_name in self._extension:
                if prop_value != self._extension[prop_name]:
                    prop.findall('value')[0].text = self._extension[prop_name]
                    logger.debug("Setting %s to %s", prop_name, prop[1].text)

                properties_to_set.remove(prop_name)

            if prop_name in self._removals:
                val_to_remove = self._removals[prop_name]
                if val_to_remove in prop_value:
                    new_prop_value = prop_value.replace(val_to_remove, "")
                    if str.isspace(new_prop_value) or len(new_prop_value) == 0:
                        root.remove(prop)
                    else:
                        prop.findall('value')[0].text = new_prop_value

        for remaining_prop in properties_to_set:
            new_config_prop = Element('property')
            new_config_prop_name = Element('name')
            new_config_prop_name.text = remaining_prop
            new_config_value = Element('value')
            new_config_value.text = self._extension[remaining_prop]

            logger.debug("Adding new property %s with value %s", remaining_prop, new_config_value.text)

            new_config_prop.append(new_config_prop_name)
            new_config_prop.append(new_config_value)

            root.append(new_config_prop)

    def commit(self):
        self._base_xml.write(self._file.val)

    def to_str(self) -> str:
        root = self._get_root()
        ET.indent(root, space="\t", level=0)
        return str(ET.tostring(root, encoding='unicode', method='xml'))

    def _get_root(self):
        if hasattr(self._base_xml, "tag"):
            root = self._base_xml
        else:
            root = self._base_xml.getroot()
        return root

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__iter__()}


class HadoopPropertiesConfig(HadoopConfigBase):
    def __init__(self, file: HadoopConfigFile, base_path: str = None):
        self._file = file
        self._extension: Dict[str, str] = {}
        if base_path:
            self._base_conf = self._read_file(base_path)
        else:
            self._base_conf = None

    @staticmethod
    def _read_file(f: str):
        config_parser = configparser.RawConfigParser(strict=False)
        config_parser.optionxform = str
        with open(f) as fp:
            config_parser.read_file(itertools.chain(['[global]'], fp), source=f)
        return config_parser

    def __iter__(self) -> Iterator[Tuple[str, str]]:
        if self._base_conf:
            return self._get_all_props_as_dict().items().__iter__()
        else:
            return self._extension.items().__iter__()

    @property
    def file(self) -> str:
        return str(self._file.val)

    @property
    def properties(self) -> ElementTree:
        return self._base_conf

    def set_base_config(self, path: str):
        self._base_conf = self._read_file(path)

    def extend_with_args(self, args: Dict[str, str]):
        self._extension.update(args)

    def merge(self):
        if not self._base_conf:
            raise HadesException("Can not merge without base properties. Set base properties before calling merge.")

        properties_to_set = set(self._extension.keys())
        all_items_dict = self._get_all_props_as_dict()

        self._base_conf['extension'] = {}
        ext_section = self._base_conf['extension']
        for prop_name in properties_to_set:
            if prop_name in all_items_dict:
                new_prop_value = self._extension[prop_name]
                old_prop_value = all_items_dict[prop_name]
                logger.debug("Setting %s to %s. Old value was: %s", prop_name, new_prop_value, old_prop_value)
                ext_section[prop_name] = self._extension[prop_name]
            else:
                prop_value = self._extension[prop_name]
                logger.debug("Adding new property %s with value %s", prop_name, prop_value)
                ext_section[prop_name] = prop_value

    def _get_all_props_as_dict(self):
        all_items: List[Tuple[str, str]] = [i for i in self._base_conf['global'].items()]
        all_items_dict = {i[0]: i[1] for i in all_items}
        return all_items_dict

    def commit(self):
        with open(self._file.val, 'w') as configfile:
            self._base_conf.write(configfile)

    def to_str(self) -> str:
        return str({section: dict(self._base_conf[section]) for section in self._base_conf.sections()})

    def to_dict(self) -> dict:
        return self._get_all_props_as_dict()


class HadoopShellScriptConfig(HadoopConfigBase):
    def __init__(self, file: HadoopConfigFile, base_path: str = None):
        self._file = file
        self._extension: Dict[str, str] = {}
        self._removals: Dict[str, str] = {}
        if base_path:
            self._base_conf: List[str] = self._read_file(base_path)
        else:
            self._base_conf = None

    @staticmethod
    def _read_file(f: str):
        with open(f) as file:
            lines = [line.rstrip() for line in file]
        return lines

    def __iter__(self) -> Iterator[str]:
        if self._base_conf:
            return self._base_conf.__iter__()
        else:
            return self._extension.__iter__()

    @property
    def file(self) -> str:
        return str(self._file.val)

    def set_base_config(self, path: str):
        self._base_conf = self._read_file(path)

    def extend_with_args(self, args: Dict[str, str]):
        self._extension.update(args)

    def remove_confs(self, confs: Dict[str, str]):
        self._removals = confs

    def merge(self):
        if not self._base_conf:
            raise HadesException("Can not merge without base config. Set base cconfig before calling merge.")
        # Don't need to do anything fancy here, commit will handle it on its own

    def commit(self):
        base_conf_set = set(self._base_conf)
        removals = set([f"{k}={v}" for k, v in self._removals.items()])
        extensions = set([f"{k}={v}" for k, v in self._extension.items()])

        with open(self._file.val, 'w') as configfile:
            for line in self._base_conf:
                if line not in removals:
                    # Do not write line from base conf if we want to remove it from the file
                    configfile.write(line + "\n")

            # Do not write Separator if it's already in base conf
            if EXTENSION_SEPARATOR not in base_conf_set:
                configfile.write("\n\n" + EXTENSION_SEPARATOR)

            for line in extensions:
                # Do not write line again if it's already in base conf
                if line not in base_conf_set:
                    configfile.write(line + "\n")

    def to_str(self) -> str:
        return "\n".join(self._base_conf) + EXTENSION_SEPARATOR + "\n".join(self._extension)

    def to_dict(self) -> dict:
        raise NotImplementedError("This is intentionally not implemented for this config type!")
