from dataclasses import dataclass, field
from typing import List
from colr import color


@dataclass
class QueueNode:
    name: str
    capacity: int
    weight: float
    is_dynamic: bool
    children: List['QueueNode'] = field(default_factory=list)

    def __str__(self):
        attrs = "[c: {}]".format(self.capacity) if self.capacity else "[w: {}]".format(self.weight)
        return "{} {}".format(self.name, attrs) \
            if not self.is_dynamic else color(fore='blue', text="{} {}".format(self.name, attrs))

    def __hash__(self):
        return hash(self.name)


class CapacitySchedulerQueue:
    DYNAMIC_LEGACY = "dynamicLegacy"
    DYNAMIC_FLEXIBLE = "dynamicFlexible"

    def __init__(self, root: QueueNode):
        self._root = root

    @classmethod
    def from_rm_api_data(cls, data: dict) -> 'CapacitySchedulerQueue':
        root_data = data['scheduler']['schedulerInfo']
        return CapacitySchedulerQueue(cls._traverse(root_data))

    @staticmethod
    def _traverse(queue_data) -> QueueNode:
        creation_method = queue_data.get('creationMethod', '')
        q = QueueNode(name=queue_data['queueName'], capacity=queue_data['capacity'], weight=queue_data.get('weight', -1),
                      is_dynamic=creation_method == CapacitySchedulerQueue.DYNAMIC_FLEXIBLE
                                 or creation_method == CapacitySchedulerQueue.DYNAMIC_LEGACY)
        if 'queues' in queue_data:
            q.children.extend([CapacitySchedulerQueue._traverse(iq) for iq in queue_data['queues']['queue']])

        return q

    def get_root(self) -> QueueNode:
        return self._root