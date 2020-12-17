from unittest import TestCase

from hadoop.cluster import HadoopCluster
from hadoop.cluster_type import ClusterType
from hadoop.role import HadoopRoleInstance, HadoopRoleType
from hadoop.selector import HadoopRoleSelector
from hadoop.service import YarnService, HdfsService


class TestHadoopRoleSelector(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        services = [YarnService(None, "Yarn-Test", {
            "ResourceManager-Test": HadoopRoleInstance("Yarn-Test", None, "ResourceManager-Test", HadoopRoleType.RM),
            "ResourceManager-Test-2": HadoopRoleInstance("Yarn-Test", None, "ResourceManager-Test-2", HadoopRoleType.RM),
            "NodeManager-Test": HadoopRoleInstance("Yarn-Test", None, "NodeManager-Test", HadoopRoleType.NM)
        }),
                    HdfsService(None, "Hdfs-Test", {
                        "NameNode-Test": HadoopRoleInstance("Hdfs-Test", None, "NameNode-Test", HadoopRoleType.NN)
                    })]
        cls.cluster = HadoopCluster(None, ClusterType.HADOCK, services)
        cls.selector = HadoopRoleSelector(cls.cluster.get_services())

    def test_select_default(self):
        roles = self.selector.select("Yarn/ResourceManager")

        self.assertEqual(len(roles), 2)

    def test_select_all(self):
        roles = self.selector.select("")

        self.assertEqual(len(roles), 4)

    def test_select_service_only(self):
        roles = self.selector.select("Yarn")

        self.assertEqual(len(roles), 3)

    def test_extended(self):
        roles = self.selector.select("name=Yarn-Test&type=Yarn/name=ResourceManager-Test-2")

        self.assertEqual(len(roles), 1)