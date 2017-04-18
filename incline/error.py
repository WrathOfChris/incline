class InclineError(Exception):
    pass


class InclineInterface(InclineError):
    pass


class InclineNotFound(InclineError):
    pass


class InclineExists(InclineError):
    pass


class InclineDataError(InclineError):
    pass
