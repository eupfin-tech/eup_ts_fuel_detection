from functools import wraps
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
import os

def _parse_otel_resource_attributes():
    """解析 OTEL_RESOURCE_ATTRIBUTES 環境變數"""
    otel_attrs = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "")
    if not otel_attrs:
        return {}
    
    attributes = {}
    for pair in otel_attrs.split(","):
        if "=" in pair:
            key, value = pair.split("=", 1)
            attributes[key.strip()] = value.strip()
    return attributes


def init_observability(app, *, service_name: str = "eup-fuel-modeling-api"):
    if os.getenv("MODELING_EXPORTER", "false").lower() != "true":
        return
    
    if "service.name" not in otel_attributes:
        raise ValueError(
            "OTEL_RESOURCE_ATTRIBUTES must contain service.name."
            "Please ensure the tempo-internal variable group is correctly linked to the pipeline,"
            f"currently OTEL_RESOURCE_ATTRIBUTES = '{os.getenv('OTEL_RESOURCE_ATTRIBUTES', 'NOT_SET')}'"
        )
    
    otel_attributes = _parse_otel_resource_attributes()
    resource_attributes = {"service.name": service_name}
    resource_attributes.update(otel_attributes) 
    
    resource = Resource.create(resource_attributes)
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor().instrument_app(app)
    RequestsInstrumentor().instrument()
    

def traced_query_sql(fn):
    @wraps(fn)
    def wrapper(sql: str, *args, **kwargs):
        sql_preview = (sql or "")[:300].replace("\n", " ")
        with trace.get_tracer("db").start_as_current_span(
            "mssql.query",
            attributes={
                "db.system": "mssql",
                "db.statement": sql_preview,
            },
        ):
            return fn(sql, *args, **kwargs)
    return wrapper