from unittest import TestCase

from hadoop.hadock.executor import HadockExecutor


class TestHadockExecutor(TestCase):
    def test_discover(self):
        executor = HadockExecutor(".", "test-compose.yml")
        cluster = executor.discover()
        self.assertGreater(len(cluster.context.keys()), 0, "Context is not converted")
