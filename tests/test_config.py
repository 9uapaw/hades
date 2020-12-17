from unittest import TestCase

from core.config import Config


class TestConfig(TestCase):
    TEST_CONFIG_FILE = "test_config.json"

    def test_from_json(self):
        config: Config = None

        with open(self.TEST_CONFIG_FILE, 'r') as f:
            json_str = f.read()
            config = Config.from_json(json_str)

        self.assertIsNotNone(config)
        self.assertTrue(config.cluster.context.keys())
