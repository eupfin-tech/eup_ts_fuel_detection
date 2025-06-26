from functools import wraps
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
import os

def init_observability(app, *, service_name: str = "eup-fuel-modeling-api"):
    if os.getenv("MODELING_EXPORTER", "false").lower() != "true":
        return
    
    resource = Resource.create({"service.name": service_name})
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