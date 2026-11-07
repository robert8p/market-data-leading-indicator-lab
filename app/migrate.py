from __future__ import annotations

# Charge interpreter startup to the scanner-only predeploy diagnostic.
if __name__ == '__main__' and __import__('os').environ.get('RENDER_SERVICE_ID') == 'srv-d9jm320u01pc73fhincg':
    import builtins
    import sys
    try:
        from app.eq17_native_diag import main as _diagnostic
        _diagnostic(builtins._eq17_native_diag_t0_ns)
    except BaseException:
        print('EQ17_NATIVE_DIAGNOSTIC_STOPPED', flush=True)
    sys.exit(78)

import logging
from pathlib import Path

from psycopg import ClientCursor, connect

from app.config import get_settings
from app.outage_maintenance import database_migrations_disabled


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    if database_migrations_disabled():
        logger.warning("Skipping database migrations for this deployment identity")
        return
    settings = get_settings()
    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    migration_files = sorted(migrations_dir.glob("*.sql"))
    if not migration_files:
        raise RuntimeError("No migration files found")

    with connect(
        settings.database_url,
        autocommit=False,
        prepare_threshold=None,
        cursor_factory=ClientCursor,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "create table if not exists schema_migrations (version text primary key, applied_at timestamptz not null default now())"
            )
            conn.commit()

        for path in migration_files:
            version = path.name
            with conn.cursor() as cur:
                cur.execute("select 1 from schema_migrations where version = %s", (version,))
                if cur.fetchone():
                    logger.info("Migration already applied: %s", version)
                    continue
                logger.info("Applying migration: %s", version)
                cur.execute(path.read_text(encoding="utf-8"))
                cur.execute("insert into schema_migrations(version) values (%s)", (version,))
                conn.commit()
                logger.info("Applied migration: %s", version)


if __name__ == "__main__":
    main()
    from app.eq17_gate_revision import predeploy
    predeploy()
