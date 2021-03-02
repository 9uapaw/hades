from unittest import TestCase

from hadoop.host import RemoteHostInstance
from hadoop.role import HadoopRoleInstance, HadoopRoleType


class TestRemoteHostInstance(TestCase):

    def setUp(self):
        self.host = RemoteHostInstance(HadoopRoleInstance(None, "test", HadoopRoleType.RM, None), "test", "admin")

    def test_make_backup(self):
        cmd = self.host.make_backup("/var/tmp/test.py")
