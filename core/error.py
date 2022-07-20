from typing import List


class HadesException(Exception):
    pass


class ConfigSetupException(HadesException):

    def __init__(self, msg: str, attr: str = ""):
        self._msg = msg
        self._attr = attr

    def __str__(self):
        return "{}: {}".format(self._attr, self._msg) if self._attr else self._msg


class CliArgException(HadesException):
    pass


class CommandExecutionException(HadesException):

    def __init__(self, msg: str, cmd: str = None, stderr: List[str] = None, stdout: List[str] = None):
        self._msg = msg
        self._cmd = cmd
        self.stdout = stdout if stdout else []
        self.stderr = stderr if stderr else []

    def __str__(self):
        return "{}: {}\n Command: {} \n stderr: {} \n stdout: {}".format(
            self.__class__.__name__, self._msg, self._cmd if self._cmd else "", "\n".join(self.stderr), "\n".join(self.stdout))


class MultiCommandExecutionException(HadesException):
    def __init__(self, exceptions: List[CommandExecutionException]):
        self._exceptions = exceptions

    def __str__(self):
        return "{}: {}".format(self.__class__.__name__, self._exceptions)


class SelectorException(HadesException):
    pass
