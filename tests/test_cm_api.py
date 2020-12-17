from unittest import TestCase

from hadoop.cm.cm_api import CmApi


class TestCmApi(TestCase):

    def test_api(self):
        host = "http://gandras-1.gandras.root.hwx.site:7180"
        cm_api = CmApi(host)

        clusters = cm_api.get_clusters()
        print(clusters)
