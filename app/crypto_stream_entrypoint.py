from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone

from app.crypto_catalogue_bootstrap import refresh_crypto_venue_catalogue
from app.db import db_connection, fetch_one


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)

_STREAM_LOCK_NAME = "market-data-crypto-stream-singleton-v1"
_STREAM_LOCK_RETRY_SECONDS = 0.5
_STREAM_LOCK_LOG_EVERY_ATTEMPTS = 20


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
    """Make the stream self-starting without letting provider throttling block capture."""
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
        except Exception as exc:  # bounded retry only when no usable catalogue exists
            last_error = exc
            if tradable_count > 0:
                logger.warning(
                    "Crypto venue catalogue refresh failed; starting from cached catalogue "
                    "tradable=%s latest_seen=%s error=%s",
                    tradable_count,
                    latest_seen.isoformat() if latest_seen else None,
                    exc,
                )
                return
            logger.exception("Crypto venue catalogue bootstrap failed on attempt %s", attempt)
            if attempt < 6:
                time.sleep(min(60, 5 * attempt))

    raise RuntimeError("Unable to bootstrap crypto venue catalogue") from last_error


def _run_singleton_stream() -> None:
    """Hold a session advisory lock only while this process owns live capture.

    Render briefly overlaps old and new workers during zero-downtime deploys. The
    singleton prevents duplicate capture, while a short lock retry interval lets a
    replacement assume ownership almost immediately after the retiring process
    releases the lock on SIGTERM. This limits unavoidable deploy handoff gaps even
    when the service is redeployed by an unrelated repository commit.
    """
    with db_connection() as lock_conn:
        acquired = False
        attempts = 0
        while not acquired:
            attempts += 1
            with lock_conn.cursor() as cur:
                cur.execute(
                    "select pg_try_advisory_lock(hashtext(%s)::bigint) as acquired",
                    (_STREAM_LOCK_NAME,),
                )
                row = cur.fetchone() or {}
                acquired = bool(row.get("acquired"))
            lock_conn.commit()
            if not acquired:
                if attempts == 1 or attempts % _STREAM_LOCK_LOG_EVERY_ATTEMPTS == 0:
                    logger.info(
                        "Another crypto stream worker is still active; retrying singleton lock "
                        "attempt=%s interval=%.1fs",
                        attempts,
                        _STREAM_LOCK_RETRY_SECONDS,
                    )
                time.sleep(_STREAM_LOCK_RETRY_SECONDS)

        logger.info("Crypto stream singleton lock acquired after %s attempt(s)", attempts)
        ensure_crypto_catalogue()
        import app.crypto_stream as crypto_stream
        from app.crypto_stream_deadlock_fixes import install_crypto_stream_deadlock_fixes
        from app.crypto_stream_runtime_fixes import install_crypto_stream_runtime_fixes

        install_crypto_stream_runtime_fixes(crypto_stream)
        install_crypto_stream_deadlock_fixes(crypto_stream)
        original_signal_handler = crypto_stream._signal_handler
        lock_released = False

        def release_stream_lock() -> None:
            nonlocal lock_released
            if lock_released:
                return
            try:
                with lock_conn.cursor() as cur:
                    cur.execute(
                        "select pg_advisory_unlock(hashtext(%s)::bigint) as released",
                        (_STREAM_LOCK_NAME,),
                    )
                    row = cur.fetchone() or {}
                lock_conn.commit()
                lock_released = True
                logger.info("Crypto stream singleton lock released for deployment handoff: %s", row.get("released"))
            except Exception:
                # If the session itself is already closing, PostgreSQL releases the
                # session advisory lock automatically. Never block shutdown on this.
                logger.exception("Unable to explicitly release crypto stream singleton lock")

        def handoff_signal_handler() -> None:
            # Stop capture first, then let the replacement acquire the lock while
            # this worker finishes idempotent buffered writes in its normal finally.
            original_signal_handler()
            release_stream_lock()

        crypto_stream._signal_handler = handoff_signal_handler
        try:
            crypto_stream.main()
        finally:
            release_stream_lock()


def main() -> None:
    _run_singleton_stream()


if __name__ == "__main__":
    main()
