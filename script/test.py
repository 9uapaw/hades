from script.base import HadesScriptBase


class TestScript(HadesScriptBase):

    def run(self):
        print(self.cluster.get_status())