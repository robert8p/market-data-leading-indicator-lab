from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from app.crypto_catalogue_bootstrap import refresh_crypto_venue_catalogue
from app.db import fetch_one


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def _catalogue_state() -> tuple[int, datetime | None]:
    row = fetch_one(
        """
        select count(*) filter (where tradable=true)::int as tradable_count,
               max(last_seen_at) filter (where tradable=true) as latest_seen
          from crypto_venue_symbols
        """
    ) or {}
    return int(row.get("tradable_count") or 0), row.get("latest_seen")


def ensure_crypto_catalogue() -> None:
    """Make the stream self-starting instead of depending on a prior batch job."""
    tradable_count, latest_seen = _catalogue_state()
    stale_before = datetime.now(timezone.utc) - timedelta(hours=6)
    if tradable_count > 0 and latest_seen is not None and latest_seen >= stale_before:
        logger.info(
            "Crypto venue catalogue ready: %s tradable mappings, latest_seen=%s",
            tradable_count,
            latest_seen.isoformat(),
        )
        return

    last_error: Exception | None = None
    for attempt in range(1, 7):
        try:
            logger.info(
                "Refreshing crypto venue catalogue before stream startup: attempt=%s existing=%s latest_seen=%s",
                attempt,
                tradable_count,
                latest_seen.isoformat() if latest_seen else None,
            )
            refreshed = refresh_crypto_venue_catalogue()
            tradable_count, latest_seen = _catalogue_state()
            if refreshed <= 0 or tradable_count <= 0:
                raise RuntimeError("Crypto venue catalogue refresh returned zero tradable mappings")
            logger.info(
                "Crypto venue catalogue populated: refreshed=%s tradable=%s latest_seen=%s",
                refreshed,
                tradable_count,
                latest_seen.isoformat() if latest_seen else None,
            )
            return
        except Exception as exc:  # bounded startup retry; failure must be visible
            last_error = exc
            logger.exception("Crypto venue catalogue bootstrap failed on attempt %s", attempt)
            if attempt < 6:
                time.sleep(min(60, 5 * attempt))

    raise RuntimeError("Unable to bootstrap crypto venue catalogue") from last_error


def main() -> None:
    ensure_crypto_catalogue()
    import app.crypto_stream as crypto_stream
    from app.crypto_stream_runtime_fixes import install_crypto_stream_runtime_fixes

    install_crypto_stream_runtime_fixes(crypto_stream)
    crypto_stream.main()


if __name__ == "__main__":
    main()
