import unittest
import incline
from incline.InclineTrace import InclineTrace
from incline.InclineTraceConsole import InclineTraceConsole

TEST_PREFIX="test-InclineTrace"

trace = InclineTrace()

class TestInclineTrace(unittest.TestCase):
    maxDiff = None

    def test_class(self):
        pass

    def test_span(self):
        with trace.span(f"{TEST_PREFIX}-span") as span:
            span.set_attribute("test.test_span", True)

    def test_start_as_current_span(self):
        with trace.tracer.start_as_current_span(
                f"{TEST_PREFIX}-start_as_current_span") as span:
            span.set_attribute("test.test_start_as_current_span", True)

if __name__ == "__main__":
    # opentelemetry traces to console
    trace = InclineTraceConsole()

    unittest.main()
