FROM python:3.11-slim AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      gcc \
      python3-dev \
      libpq-dev \
 && rm -rf /var/lib/apt/lists/*


COPY requirements.txt .
RUN python -m pip install --no-cache-dir -r requirements.txt \
 && python -m pip install --no-cache-dir \
      opentelemetry-api==1.34.1 \
      opentelemetry-sdk==1.34.1 \
      opentelemetry-exporter-otlp-proto-grpc==1.34.1 \
      opentelemetry-instrumentation-fastapi==0.55b1 \
      opentelemetry-instrumentation-requests==0.55b1 \
      opentelemetry-util-http==0.55b1

WORKDIR /app
COPY . .

CMD ["opentelemetry-instrument", "python", "event_comparator.py"]
