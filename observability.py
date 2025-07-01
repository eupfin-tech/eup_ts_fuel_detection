import os
import logging
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter, SpanExporter, SpanExportResult
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.requests import RequestsInstrumentor


FastAPIInstrumentor = None  #照理來說應該沒這行，但我怕沒把你的FastAPI刪乾淨

_log = logging.getLogger("observability")

def _bool(v: str, default: bool = False) -> bool:
    return {"true": True, "false": False, "1": True, "0": False}.get(v.lower(), default)


def _parse_otel_resource_attributes() -> dict:
    raw = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "")
    attrs = {}
    for item in filter(None, (part.strip() for part in raw.split(","))):
        if "=" in item:
            k, v = item.split("=", 1)
            attrs[k.strip()] = v.strip()
    return attrs


def _default_service_name() -> str:
    return os.getenv("PROJECT_NAME", "unknown") + "-" + os.getenv("STAGE", "dev")


class DebugSpanExporter(SpanExporter):
    def export(self, spans):
        print("[OTEL-DEBUG] Exporting spans:")
        for span in spans:
            print(f"[OTEL-DEBUG] Span: name={span.name}, start={span.start_time}, end={span.end_time}, attrs={span.attributes}")
        return SpanExportResult.SUCCESS
    def shutdown(self):
        print("[OTEL-DEBUG] DebugSpanExporter shutdown")
        return None


def init_observability(app: Optional["FastAPI"] = None) -> None:
    attrs = _parse_otel_resource_attributes()
    attrs.setdefault("service.name", _default_service_name())

    resource = Resource.create(attrs)

    traces_exporter = os.getenv("OTEL_TRACES_EXPORTER", "otlp").lower()
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    insecure = _bool(os.getenv("OTEL_EXPORTER_OTLP_INSECURE", "false"))

    print(f"[OTEL-DEBUG] init_observability called, exporter={traces_exporter}, endpoint={endpoint}, insecure={insecure}")

    if traces_exporter == "none":
        _log.warning("OTEL_TRACES_EXPORTER=none → tracing disabled")
        print("[OTEL-DEBUG] Tracing disabled")
        return 

    provider = TracerProvider(resource=resource)

    try:
        if traces_exporter == "console":
            print("[OTEL-DEBUG] Using ConsoleSpanExporter")
            exporter = ConsoleSpanExporter()
        elif traces_exporter == "debug":
            print("[OTEL-DEBUG] Using DebugSpanExporter (prints all spans)")
            exporter = DebugSpanExporter()
        else: 
            print(f"[OTEL-DEBUG] Using OTLPSpanExporter, endpoint={endpoint}, insecure={insecure}")
            exporter = OTLPSpanExporter(endpoint=endpoint, insecure=insecure)

        print("[OTEL-DEBUG] Adding BatchSpanProcessor")
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _log.info("OpenTelemetry initialised → exporter=%s endpoint=%s insecure=%s",
                  traces_exporter, endpoint if traces_exporter == "otlp" else "-", insecure)
        print("[OTEL-DEBUG] OpenTelemetry initialised")

    except Exception as exc: 
        _log.error("Failed to init OTLP exporter, fallback to Console: %s", exc, exc_info=True)
        print(f"[OTEL-DEBUG] Failed to init OTLP exporter: {exc}, fallback to ConsoleSpanExporter")
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)

    RequestsInstrumentor().instrument()
    if app is not None and FastAPIInstrumentor is not None:
        try:
            FastAPIInstrumentor().instrument_app(app)
        except Exception as ex: 
            _log.debug("Skip FastAPI instrumentation: %s", ex)
