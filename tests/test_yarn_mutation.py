from unittest import TestCase

from hadoop.yarn.yarn_mutation import YarnMutationConfig


class TestYarnMutationConfig(TestCase):

    def setUp(self) -> None:
        self.xml = YarnMutationConfig()

    def test_add(self):
        self.xml.add_queue("root.test-queue", test="hello", test2="hello2")
        print(self.xml.dump())
