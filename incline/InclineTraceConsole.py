from incline.InclineTrace import InclineTrace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (BatchSpanProcessor,
                                            ConsoleSpanExporter)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (ConsoleMetricExporter,
                                              PeriodicExportingMetricReader)
"""
Usage:
    with <trace>.tracer.start_as_current_span(name) as span:
        ....
"""


class InclineTraceConsole(InclineTrace):

    def __init__(self, name='__name__', tracer=None, meter=None):
        self.resource = Resource(attributes={SERVICE_NAME: "incline"})

        # TracerProvider
        self.trace_provider = TracerProvider(resource=self.resource)
        self.trace_processor = BatchSpanProcessor(ConsoleSpanExporter())
        self.trace_provider.add_span_processor(self.trace_processor)

        # MetricsProvider
        self.metric_reader = PeriodicExportingMetricReader(
            ConsoleMetricExporter())
        self.meter_provider = MeterProvider(
            metric_readers=[self.metric_reader])

        self.init(name, tracer, meter)
