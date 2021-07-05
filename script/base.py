import logging
import time
from abc import ABC
from typing import Callable, Any

from hadoop.cluster import HadoopCluster


class HadesScriptBase:
    LOGGER = logging.getLogger(__name__)

    def __init__(self, cluster: HadoopCluster):
        self.cluster = cluster

    def run(self):
        raise NotImplementedError()

    def wait_until(self, poll_fn: Callable, comp_fn: Callable, poll_time: int = 1):
        while True:
            before = time.time()
            res = poll_fn()
            comp = comp_fn(res)
            self.LOGGER.info(f"Check if {res} is True")
            if res == comp:
                self.LOGGER.info("Wait is complete")
                return

            after = 1 - (time.time() - before)
            if after > 0:
                time.sleep(after)
