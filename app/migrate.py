from __future__ import annotations

import logging
from pathlib import Path

from psycopg import ClientCursor, connect

from app.config import get_settings


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
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
