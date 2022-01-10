from xml.etree.ElementTree import ElementTree, Element
from xml.etree import ElementTree as ET


class MutationRequest(object):

    def __init__(self):
        self._xml = ElementTree(element=Element("sched-conf"))

    @property
    def xml(self):
        return self._xml

    @xml.setter
    def xml(self, value: ElementTree):
        self._xml = value

    def update_queue(self, queue: str, **kwargs):
        update_queue = self._create("update-queue")
        queue_name = Element("queue-name")
        queue_name.text = queue
        update_queue.append(queue_name)
        params = Element("params")
        for k, v in kwargs.items():
            entry = Element("entry")
            key = Element("key")
            key.text = k
            value = Element("value")
            value.text = v
            entry.append(key)
            entry.append(value)
            params.append(entry)
        update_queue.append(params)

    def add_queue(self, queue: str, **kwargs):
        add_queue = self._create("add-queue")
        queue_name = Element("queue-name")
        queue_name.text = queue
        add_queue.append(queue_name)
        params = Element("params")
        for k, v in kwargs.items():
            entry = Element("entry")
            key = Element("key")
            key.text = k
            value = Element("value")
            value.text = v
            entry.append(key)
            entry.append(value)
            params.append(entry)
        add_queue.append(params)

    def remove_queue(self, queue: str):
        remove_queue = Element("remove-queue")
        remove_queue.text = queue
        self.xml.getroot().append(remove_queue)

    def global_update(self, key: str, value: str):
        global_updates = self._get_or_create("global-updates")
        entry = Element("entry")
        k = Element("key")
        k.text = key
        v = Element("value")
        v.text = value
        entry.append(k)
        entry.append(v)
        global_updates.append(entry)

    def dump_xml(self, pretty: bool=False):
        if pretty:
            ET.indent(self.xml)
        return ET.tostring(self.xml.getroot()).decode()

    def _get_or_create(self, name: str):
        element = self._xml.getroot().find(name)
        if element is None:
            element = self._create(name)
        return element

    def _create(self, name: str):
        element = Element(name)
        self.xml.getroot().append(element)
        return element
