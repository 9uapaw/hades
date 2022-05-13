import enum
from collections import deque
from dataclasses import dataclass, field
from typing import List
from colr import color


class QueueState(enum.Enum):
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    DRAINING = "DRAINING"


@dataclass
class QueueNode:
    name: str
    path: str
    capacity: int
    used_capacity: int
    weight: float
    is_dynamic: bool
    state: QueueState
    children: List['QueueNode'] = field(default_factory=list)

    def __str__(self):
        attrs = "[c: {} - used: {}%]".format(self.capacity, self.used_capacity) if self.capacity else "[w: {}]".format(self.weight)
        name = self.name
        if self.is_dynamic:
            name = color(fore='blue', text=self.name)
        if self.state == QueueState.STOPPED:
            name = color(fore='red', text=self.name + " <STOPPED>")
        elif self.state == QueueState.DRAINING:
            name = color(fore='gray', text=self.name + " <DRAINING>")
        return "{} {}".format(name, attrs)

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
        q = QueueNode(name=queue_data['queueName'], path=queue_data['queuePath'], capacity=queue_data['capacity'], weight=queue_data.get('weight', -1),
                      is_dynamic=creation_method == CapacitySchedulerQueue.DYNAMIC_FLEXIBLE
                                 or creation_method == CapacitySchedulerQueue.DYNAMIC_LEGACY,
                      state=QueueState(queue_data.get('state', 'RUNNING')),
                      used_capacity=queue_data['usedCapacity']
                      )
        if 'queues' in queue_data:
            q.children.extend([CapacitySchedulerQueue._traverse(iq) for iq in queue_data['queues'].get('queue', [])])

        return q

    def __iter__(self):
        q = deque()
        q.append(self._root)

        while q:
            c = q.popleft()
            for child in c.children:
                q.append(child)
            yield c

    def get_root(self) -> QueueNode:
        return self._root

    def get_queue(self, name: str) -> QueueNode or None:
        return self._find(self._root, name)

    def _find(self, queue: QueueNode, name: str) -> QueueNode or None:
        if queue.name == name:
            return queue

        res = None
        for queue in queue.children:
           res = self._find(queue, name)
           if res:
               return queue

        return None

