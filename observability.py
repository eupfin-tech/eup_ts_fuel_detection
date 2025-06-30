import os
import logging
from typing import Optional

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
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


def init_observability(app: Optional["FastAPI"] = None) -> None:
    attrs = _parse_otel_resource_attributes()
    attrs.setdefault("service.name", _default_service_name())

    resource = Resource.create(attrs)

    traces_exporter = os.getenv("OTEL_TRACES_EXPORTER", "otlp").lower()
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    insecure = _bool(os.getenv("OTEL_EXPORTER_OTLP_INSECURE", "false"))

    if traces_exporter == "none":
        _log.warning("OTEL_TRACES_EXPORTER=none → tracing disabled")
        return 

    provider = TracerProvider(resource=resource)

    try:
        if traces_exporter == "console":
            exporter = ConsoleSpanExporter()
        else: 
            exporter = OTLPSpanExporter(endpoint=endpoint, insecure=insecure)

        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _log.info("OpenTelemetry initialised → exporter=%s endpoint=%s insecure=%s",
                  traces_exporter, endpoint if traces_exporter == "otlp" else "-", insecure)

    except Exception as exc: 
        _log.error("Failed to init OTLP exporter, fallback to Console: %s", exc, exc_info=True)
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        trace.set_tracer_provider(provider)

    RequestsInstrumentor().instrument()
    if app is not None and FastAPIInstrumentor is not None:
        try:
            FastAPIInstrumentor().instrument_app(app)
        except Exception as ex: 
            _log.debug("Skip FastAPI instrumentation: %s", ex)
