from unittest import TestCase

from hadoop.yarn.yarn_mutation import MutationRequest


class TestMutationApis(TestCase):

    def test_update_queue(self):
        mutation = MutationRequest()
        mutation.update_queue("root.test-queue1", test="hello1", test2="hello2")
        mutation.update_queue("root.test-queue2", test="hello3", test2="hello4")
        self.assertEqual(mutation.dump_xml(pretty=True),
            """\
<sched-conf>
  <update-queue>
    <queue-name>root.test-queue1</queue-name>
    <params>
      <entry>
        <key>test</key>
        <value>hello1</value>
      </entry>
      <entry>
        <key>test2</key>
        <value>hello2</value>
      </entry>
    </params>
  </update-queue>
  <update-queue>
    <queue-name>root.test-queue2</queue-name>
    <params>
      <entry>
        <key>test</key>
        <value>hello3</value>
      </entry>
      <entry>
        <key>test2</key>
        <value>hello4</value>
      </entry>
    </params>
  </update-queue>
</sched-conf>""")

    def test_add_queue(self):
        mutation = MutationRequest()
        mutation.add_queue("root.test-queue1", test="hello1", test2="hello2")
        mutation.add_queue("root.test-queue2", test="hello3", test2="hello4")
        self.assertEqual(mutation.dump_xml(pretty=True),
            """\
<sched-conf>
  <add-queue>
    <queue-name>root.test-queue1</queue-name>
    <params>
      <entry>
        <key>test</key>
        <value>hello1</value>
      </entry>
      <entry>
        <key>test2</key>
        <value>hello2</value>
      </entry>
    </params>
  </add-queue>
  <add-queue>
    <queue-name>root.test-queue2</queue-name>
    <params>
      <entry>
        <key>test</key>
        <value>hello3</value>
      </entry>
      <entry>
        <key>test2</key>
        <value>hello4</value>
      </entry>
    </params>
  </add-queue>
</sched-conf>""")

    def test_remove_queue(self):
        mutation = MutationRequest()
        mutation.remove_queue("root.test-queue1")
        mutation.remove_queue("root.test-queue2")
        self.assertEqual(mutation.dump_xml(pretty=True),
            """\
<sched-conf>
  <remove-queue>root.test-queue1</remove-queue>
  <remove-queue>root.test-queue2</remove-queue>
</sched-conf>""")

    def test_global_updates(self):
        mutation = MutationRequest()
        mutation.global_update("key1", "value1")
        mutation.global_update("key2", "value2")
        self.assertEqual(mutation.dump_xml(pretty=True),
            """\
<sched-conf>
  <global-updates>
    <entry>
      <key>key1</key>
      <value>value1</value>
    </entry>
    <entry>
      <key>key2</key>
      <value>value2</value>
    </entry>
  </global-updates>
</sched-conf>""")

    def test_combined_add_and_update(self):
        mutation = MutationRequest()
        mutation.add_queue("root.a", capacity="10")
        mutation.update_queue("root.default", capacity="90")
        self.assertEqual(mutation.dump_xml(pretty=True),
            """\
<sched-conf>
  <add-queue>
    <queue-name>root.a</queue-name>
    <params>
      <entry>
        <key>capacity</key>
        <value>10</value>
      </entry>
    </params>
  </add-queue>
  <update-queue>
    <queue-name>root.default</queue-name>
    <params>
      <entry>
        <key>capacity</key>
        <value>90</value>
      </entry>
    </params>
  </update-queue>
</sched-conf>""")
