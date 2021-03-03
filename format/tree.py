import pptree

from rich import print
from rich.text import Text
from rich.tree import Tree
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
        tree = Tree(Text(str(self._root) + "\n│"))
        self._traverse(tree, self._root, True)

        with _Capturing() as output:
            print(tree)

        return "\n".join(output)

    def _traverse(self, tree: Tree, node, is_root: bool = False):
        if node.children:
            text = Text(str(node) + "\n│")
        else:
            text = Text(str(node) + "\n")

        if not is_root:
            branch = tree.add(text)
        else:
            branch = tree

        for child in node.children:
            self._traverse(branch, child, False)
