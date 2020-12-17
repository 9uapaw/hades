from dataclasses import dataclass, field
from typing import List


@dataclass
class QueueNode:
    name: str
    capacity: int
    children: List['QueueNode'] = field(default_factory=list)

    def __str__(self):
        return "{}: {}".format(self.name, self.capacity)

    def __hash__(self):
        return hash(self.name)


class CapacitySchedulerQueue:

    def __init__(self, root: QueueNode):
        self._root = root

    @classmethod
    def from_rm_api_data(cls, data: dict) -> 'CapacitySchedulerQueue':
        root_data = data['scheduler']['schedulerInfo']
        return CapacitySchedulerQueue(cls._traverse(root_data))

    @staticmethod
    def _traverse(queue_data) -> QueueNode:
        q = QueueNode(name=queue_data['queueName'], capacity=queue_data['capacity'])
        if 'queues' in queue_data:
            q.children.extend([CapacitySchedulerQueue._traverse(iq) for iq in queue_data['queues']['queue']])

        return q

    def get_root(self) -> QueueNode:
        return self._root