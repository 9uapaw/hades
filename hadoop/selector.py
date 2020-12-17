"""
Wraps a selector expression to choose role instances from a cluster.
Format:
Default: <SERVICE_TYPE>?/<ROLE_TYPE>?
Extended: name=<SERVICE_NAME>?&type=<SERVICE_TYPE>/name=<ROLE_NAME>?&type=<ROLE_TYPE>
"""
from dataclasses import dataclass
from typing import List, Dict

import hadoop.cluster
from core.error import SelectorException
from hadoop.role import HadoopRoleInstance
from hadoop.service import HadoopService


@dataclass
class SelectorFragment:
    name: str = ''
    fragment_type: str = ''


class HadoopRoleSelector:
    DELIMITER = "/"
    INNER_DELIMITER = "&"
    NAME_FIELD = "name="
    TYPE_FIELD = "type="

    def __init__(self, services: List[HadoopService]):
        self._services = services

    def select(self, selector: str) -> List[HadoopRoleInstance]:
        selectors = self._interpret_selector(selector)
        services = self._select_services(self._services, selectors[0])
        selected_roles = []
        for service in services:
            selected_roles.extend(self._select_roles(service.get_roles(), selectors[1]))

        return selected_roles

    def _interpret_selector(self, selector: str) -> (SelectorFragment, SelectorFragment):
        if not selector:
            return SelectorFragment(), SelectorFragment()

        if self.DELIMITER not in selector:
            return SelectorFragment(fragment_type=selector), SelectorFragment()

        fragments = selector.split(self.DELIMITER)
        if len(fragments) == 2:
            return self._extract_fragments(fragments[0]), self._extract_fragments(fragments[1])

        raise SelectorException("Invalid selector expression {}".format(selector))

    def _extract_fragments(self, fragment: str) -> SelectorFragment:
        if self.INNER_DELIMITER not in fragment:
            return SelectorFragment(fragment_type=fragment)

        inner_fragments = fragment.split(self.INNER_DELIMITER)
        selector_fragment = SelectorFragment()

        for field in inner_fragments:
            if field.startswith(self.NAME_FIELD):
                selector_fragment.name = field.replace(self.NAME_FIELD, "")
            elif field.startswith(self.TYPE_FIELD):
                selector_fragment.fragment_type = field.replace(self.TYPE_FIELD, "")

        return selector_fragment

    def _select_services(self, services: List[HadoopService], selector: SelectorFragment) -> List[HadoopService]:
        services_res = []
        for service in services:
            if not selector.fragment_type or service.service_type.value.lower() == selector.fragment_type.lower():
                services_res.append(service)

        return services_res

    def _select_roles(self, roles: Dict[str, HadoopRoleInstance], selector: SelectorFragment) -> List[
        HadoopRoleInstance]:
        role_res = []
        for role_name, role in roles.items():
            if not selector.fragment_type and not selector.name or selector.name == role_name or selector.fragment_type.lower() == role.role_type.value.lower():
                role_res.append(role)

        return role_res
