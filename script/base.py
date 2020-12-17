from abc import ABC

from hadoop.cluster import HadoopCluster


class HadesScriptBase:

    def __init__(self, cluster: HadoopCluster):
        self.cluster = cluster

    def run(self):
        raise NotImplementedError()