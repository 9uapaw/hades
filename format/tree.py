import pptree
from io import StringIO
import sys

from format.cli_formatter import CliFormat


class _Capturing(list):

    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout


class TreeFormat(CliFormat):

    def __init__(self, root):
        self._root = root

    def format(self) -> str:
        o = []
        with _Capturing() as output:
            pptree.print_tree(self._root, nameattr="", horizontal=False)
            o = output

        return "\n".join(output)
