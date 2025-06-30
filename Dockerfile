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
      opentelemetry-api==1.21.0 \
      opentelemetry-sdk==1.21.0 \
      opentelemetry-exporter-otlp==1.21.0 \
      opentelemetry-instrumentation-requests==0.42b0

WORKDIR /app
COPY . .

CMD ["opentelemetry-instrument", "python", "event_comparator.py"]
