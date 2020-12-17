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

    def __init__(self, msg: str, cmd: str = None):
        self._msg = msg
        self._cmd = cmd

    def __str__(self):
        return "{}: {}\n Command: {}".format(self.__class__.__name__, self._msg, self._cmd if self._cmd else "")


class SelectorException(HadesException):
    pass
