from unittest import TestCase

from core.config import ClusterConfig, ClusterRoleConfig, ClusterContextConfig
from hadoop.cluster import HadoopCluster
from hadoop.cluster_type import ClusterType


class TestHadoopCluster(TestCase):

    def test_from_config(self):
        roles = {
            "Test": ClusterRoleConfig("ResourceManager", "test_host")
        }
        services = {
            "Yarn": ClusterContextConfig("test_service", roles)
        }
        cluster_config = ClusterConfig(ClusterType.CM.value, services)
        cluster = HadoopCluster.from_config(cluster_config, None)
