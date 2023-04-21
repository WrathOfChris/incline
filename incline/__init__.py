# type: ignore
import logging
import importlib.metadata

__version__ = importlib.metadata.version("incline")

INCLINE_TXN_MULTIPLY = 1000000000
INCLINE_TXN_QUANTIZE = "1.000000"
INCLINE_TXN_BASEJUST = 11


# Set up logging to ``/dev/null`` like a library is supposed to.
# http://docs.python.org/3.3/howto/logging.html#configuring-logging-for-a-library
class NullHandler(logging.Handler):

    def emit(self, record):
        pass


logging.getLogger('incline').addHandler(NullHandler())
