FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
USER appuser

# Render's market-data-crypto-stream service currently inherits the image CMD
# instead of its Blueprint dockerCommand. Route by the existing service-specific
# CRYPTO_STREAM_ENABLED flag so the stream worker starts correctly while web
# services continue to serve the B-001 control UI. Explicit worker commands
# (for example python -m app.worker) continue to override this CMD.
CMD ["sh", "-c", "if [ \"${CRYPTO_STREAM_ENABLED:-false}\" = \"true\" ]; then exec python -m app.crypto_stream; else exec uvicorn app.b001_web:app --host 0.0.0.0 --port ${PORT:-10000}; fi"]
