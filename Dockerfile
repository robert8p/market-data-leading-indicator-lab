FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .
# Preserve the deployed export-only private code pins.
RUN python scripts/eq20_export_runtime_pins.py
# Isolate only the selected worker's unchanged controller, not its native gates.
RUN python scripts/eq20_isolation_image.py
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser

# The crypto stream service inherits the image CMD rather than its Blueprint
# dockerCommand. Route by the service-specific flag, and bootstrap the venue
# catalogue before opening feeds. Explicit Render worker commands still override
# this image CMD for the normal collection worker.
CMD ["sh", "-c", "if [ \"${CRYPTO_STREAM_ENABLED:-false}\" = \"true\" ]; then exec python -m app.crypto_stream_entrypoint; else exec uvicorn app.b001_web:app --host 0.0.0.0 --port ${PORT:-10000}; fi"]
