from xml.etree.ElementTree import ElementTree, Element
from xml.etree import ElementTree as ET


class YarnMutationConfig:

    def __init__(self):
        self._xml = ElementTree(element=Element("sched-conf"))
        self._update_queue = Element("update-queue")
        self._xml.getroot().append(self._update_queue)

    def add_queue(self, queue: str, **kwargs):
        queue_name = Element("queue-name")
        queue_name.text = queue
        self._update_queue.append(queue_name)
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
        self._update_queue.append(params)

    def dump(self) -> str:
        return ET.tostring(self._xml.getroot()).decode()
