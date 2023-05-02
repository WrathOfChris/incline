from opentelemetry import trace
from opentelemetry import metrics
"""
Usage:
    with <trace>.tracer.start_as_current_span("name") as span:
        ...
        span.set_attribute("key", "value")
"""


class InclineTrace(object):

    def __init__(self, name=__name__, tracer=None, meter=None):
        self.init(name, tracer, meter)

    def init(self, name, tracer, meter):
        if hasattr(self, 'trace_provider'):
            trace.set_tracer_provider(self.trace_provider)
        if hasattr(self, 'meter_provider'):
            metrics.set_meter_provider(self.meter_provider)

        self.tracer = tracer
        if not self.tracer:
            self.tracer = trace.get_tracer(name)

        self.meter = meter
        if not self.meter:
            self.meter = metrics.get_meter(name)

    def span(self, name):
        return self.tracer.start_as_current_span(name)
