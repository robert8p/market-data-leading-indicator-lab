FROM python:3.12-slim AS eq17_guard
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev linux-libc-dev && rm -rf /var/lib/apt/lists/*
COPY ops/eq17-native/restrict.c /tmp/eq17-native-restrict.c
RUN cc -std=c11 -O2 -Wall -Wextra -Werror /tmp/eq17-native-restrict.c -o /eq17-native-restrict

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN mkdir -p /etc/postgresql-common && printf 'create_main_cluster = false\n' > /etc/postgresql-common/createcluster.conf \
    && apt-get update && apt-get install -y --no-install-recommends postgresql pgbouncer \
    && rm -rf /var/lib/apt/lists/*
COPY --from=eq17_guard /eq17-native-restrict /usr/local/libexec/eq17-native-restrict
COPY --from=eq17_guard /tmp/eq17-native-restrict.c /usr/local/share/eq17-native/restrict.c
WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt "psycopg[binary,pool]==3.3.8" && pip install openpyxl==3.1.5

COPY . .
# Fail builds before switching traffic on invalid scanner integration code.
RUN python -m py_compile app/eq17_bootstrap.py app/eq17_release_integrity.py app/eq17_process_monitor.py app/eq17_verification_runner.py app/eq17_verifier_recovery.py \
    && python -m unittest discover -s tests -p 'test_eq17_release_integrity.py' \
    && python -m unittest discover -s tests -p 'test_eq17_process_monitor.py' \
    && python -m unittest discover -s tests -p 'test_eq17_verification_runner.py' \
    && python -m unittest discover -s tests -p 'test_eq17_verifier_recovery.py' \
    && python -m unittest discover -s tests -p 'test_eq17_caller_diagnostic.py' \
    && python -m unittest discover -s tests -p 'test_eq17_native_diag.py'
RUN useradd --create-home --uid 10001 appuser && chown -R appuser:appuser /app
RUN for path in app/__init__.py app/eq17_bootstrap.py sitecustomize.py; do \
        dir=$(dirname "$path"); base=$(basename "$path"); \
        mv "$path" "$dir/_shared_$base" && cp /app/ops/eq17-native/startup.py "$path" || exit 1; \
    done
USER appuser
RUN /usr/local/bin/python -I /app/ops/eq17-native/build_smoke.py

# The crypto stream service inherits the image CMD rather than its Blueprint
# dockerCommand. Route by the service-specific flag, and bootstrap the venue
# catalogue before opening feeds. Explicit Render worker commands still override
# this image CMD for the normal collection worker.
CMD ["sh", "-c", "if [ \"${RENDER_SERVICE_ID:-}\" = \"srv-d9jm320u01pc73fhincg\" ]; then exit 78; elif [ \"${CRYPTO_STREAM_ENABLED:-false}\" = \"true\" ]; then exec python -m app.crypto_stream_entrypoint; else exec uvicorn app.eq17_bootstrap:app --host 0.0.0.0 --port ${PORT:-10000}; fi"]
