import logging
import importlib.metadata
from typing import Any

__version__ = importlib.metadata.version("incline")


# Set up logging to ``/dev/null`` like a library is supposed to.
# http://docs.python.org/3.3/howto/logging.html#configuring-logging-for-a-library
class NullHandler(logging.Handler):

    def emit(self, record: Any) -> None:
        pass


logging.getLogger('incline').addHandler(NullHandler())
