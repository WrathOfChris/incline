import contextlib
from opentelemetry import trace
from opentelemetry import metrics
"""
Usage:
    with <trace>.tracer.start_as_current_span("name") as span:
        ...
        span.set_attribute("key", "value")
"""


class InclineTrace(object):

    def __init__(self,
                 name: str = __name__,
                 tracer: trace.Tracer | None = None,
                 meter: metrics.Meter | None = None):
        self.init(name, tracer, meter)

    def init(self, name: str, tracer: trace.Tracer | None,
             meter: metrics.Meter | None) -> None:
        if hasattr(self, 'trace_provider'):
            trace.set_tracer_provider(self.trace_provider)
        if hasattr(self, 'meter_provider'):
            metrics.set_meter_provider(self.meter_provider)

        if not tracer:
            tracer = trace.get_tracer(name)
        self.tracer = tracer

        self.meter = meter
        if not self.meter:
            self.meter = metrics.get_meter(name)

    def span(self,
             name: str) -> contextlib.AbstractContextManager[trace.span.Span]:
        return self.tracer.start_as_current_span(name)
