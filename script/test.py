from core.handler import MainCommandHandler
from script.base import HadesScriptBase


class TestScript(HadesScriptBase):

    def run(self, handler: MainCommandHandler):
        print(self.cluster.get_status())