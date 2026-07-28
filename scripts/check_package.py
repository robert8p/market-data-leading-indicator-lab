from __future__ import annotations

import compileall
from pathlib import Path


REQUIRED = [
    "Dockerfile",
    "render.yaml",
    "requirements.txt",
    "README.md",
    "ARCHITECTURE.md",
    "DEPLOYMENT.md",
    "VALIDATION_REPORT.md",
    "CHANGELOG.md",
    "migrations/001_initial.sql",
    "migrations/002_collection_enrichment.sql",
    "migrations/003_crypto_mining.sql",
    "migrations/004_multi_venue_detection.sql",
    "migrations/005_miner_first_coverage.sql",
    "app/main.py",
    "app/worker.py",
    "app/crypto_stream.py",
    "app/dynamic_detection.py",
    "app/capture.py",
    "app/aggregation.py",
    "app/enrichment.py",
    "app/storage.py",
]

FORBIDDEN_ACTIVE_FILES = [
    "app/exporter.py",
    "app/features.py",
    "app/templates/export_detail.html",
]


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    missing = [path for path in REQUIRED if not (root / path).exists()]
    if missing:
        raise SystemExit(f"Missing required files: {', '.join(missing)}")
    forbidden = [path for path in FORBIDDEN_ACTIVE_FILES if (root / path).exists()]
    if forbidden:
        raise SystemExit(f"Collection-only boundary violated by: {', '.join(forbidden)}")
    if not compileall.compile_dir(root / "app", quiet=1):
        raise SystemExit("Python compilation failed")
    render = (root / "render.yaml").read_text(encoding="utf-8")
    for command in ("python -m app.worker", "python -m app.crypto_stream", "python -m app.migrate"):
        if command not in render:
            raise SystemExit(f"Render blueprint is missing: {command}")
    for service_name in ("market-data-lab-web", "market-data-lab-worker", "market-data-crypto-stream"):
        if f"name: {service_name}" not in render:
            raise SystemExit(f"Render blueprint is missing compatible service name: {service_name}")
    print("Package structure, collection-only boundary and Python compilation: OK")


if __name__ == "__main__":
    main()
