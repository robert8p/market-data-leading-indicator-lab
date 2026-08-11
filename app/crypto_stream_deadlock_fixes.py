from __future__ import annotations

import asyncio
import hashlib
import re
import unicodedata
from typing import Any, Awaitable, Callable

from psycopg.types.json import Jsonb

from app.db import db_connection


_DEADLOCK_MARKERS = (
    "deadlock detected",
    "deadlock",
    "serialization failure",
)


def _is_retryable_db_conflict(exc: BaseException) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in _DEADLOCK_MARKERS)


def _safe_storage_segment(value: str) -> str:
    """Return a deterministic ASCII storage-path segment without losing symbol identity."""
    raw = str(value or "unknown")
    ascii_text = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", ascii_text).strip("._-")
    if cleaned == raw and cleaned:
        return cleaned[:80]
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]
    prefix = (cleaned[:48] or "symbol")
    return f"{prefix}-{digest}"


def _safe_raw_segment_upload(self: Any, segment: Any) -> None:
    """Upload deep raw segments using storage-safe deterministic path components."""
    object_path = (
        f"crypto/{_safe_storage_segment(segment.provider)}/"
        f"{_safe_storage_segment(segment.market_type)}/"
        f"{_safe_storage_segment(segment.canonical_symbol)}/"
        f"{_safe_storage_segment(segment.venue_symbol)}/"
        f"{_safe_storage_segment(segment.channel)}/"
        f"{segment.start_ts:%Y/%m/%d/%H%M}.jsonl.gz"
    )
    size, checksum = self.storage.upload_file(segment.path, object_path, "application/gzip")
    with db_connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            insert into crypto_raw_objects(
                provider,market_type,venue_symbol,canonical_symbol,channel,start_ts,end_ts,
                object_path,content_type,compression,message_count,size_bytes,checksum,status
            ) values (%s,%s,%s,%s,%s,%s,%s,%s,'application/gzip','gzip',%s,%s,%s,'uploaded')
            on conflict(object_path) do update set
                message_count=excluded.message_count,size_bytes=excluded.size_bytes,
                checksum=excluded.checksum,status='uploaded'
            """,
            (
                segment.provider,
                segment.market_type,
                segment.venue_symbol,
                segment.canonical_symbol,
                segment.channel,
                segment.start_ts,
                segment.end_ts,
                object_path,
                segment.message_count,
                size,
                checksum,
            ),
        )
        conn.commit()


def _safe_retry_preservations_sync(self: Any) -> int:
    """Retry pre-trigger snapshots with ASCII-safe object paths."""
    with self.preserve_lock:
        pending = list(self.pending_preservations.items())
    uploaded = 0
    for path, (decision, start, end, message_count) in pending:
        if not path.exists():
            with self.preserve_lock:
                self.pending_preservations.pop(path, None)
            continue
        safe_ts = decision.detected_at.strftime("%Y%m%dT%H%M%SZ")
        safe_symbol = _safe_storage_segment(decision.canonical_symbol)
        object_path = (
            f"crypto/pretrigger/{safe_symbol}/{decision.detected_at:%Y/%m/%d}/"
            f"{safe_ts}.jsonl.gz"
        )
        try:
            size, checksum = self.storage.upload_file(path, object_path, "application/gzip")
            with db_connection() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    insert into crypto_raw_objects(
                        provider,market_type,venue_symbol,canonical_symbol,channel,start_ts,end_ts,
                        object_path,content_type,compression,message_count,size_bytes,checksum,status,metadata
                    ) values ('multi_venue','broad',%s,%s,'broad_pretrigger',%s,%s,%s,
                              'application/gzip','gzip',%s,%s,%s,'uploaded',%s)
                    on conflict(object_path) do update set message_count=excluded.message_count,
                        size_bytes=excluded.size_bytes,checksum=excluded.checksum,status='uploaded',
                        metadata=excluded.metadata
                    """,
                    (
                        decision.canonical_symbol,
                        decision.canonical_symbol,
                        start,
                        end,
                        object_path,
                        message_count,
                        size,
                        checksum,
                        Jsonb(decision.as_reason()),
                    ),
                )
                conn.commit()
        except Exception:
            continue
        path.unlink(missing_ok=True)
        with self.preserve_lock:
            self.pending_preservations.pop(path, None)
        uploaded += 1
    return uploaded


def install_crypto_stream_deadlock_fixes(stream_module: Any) -> None:
    """Install DB-concurrency and raw-storage hardening for the live crypto stream.

    Broad-market trigger preservation can call ``BroadObservationStore.flush(force=True)``
    while the normal aggregate heartbeat is also flushing the same store. Those calls
    used separate database transactions and could lock overlapping upsert rows in a
    different order. The resulting PostgreSQL deadlock bubbled up into the scanner loop,
    forcing otherwise healthy venue connections to reconnect.

    The same stream can legitimately encounter symbols with non-Latin characters. Raw
    Supabase Storage object keys are therefore normalised to deterministic ASCII segments
    while the original canonical/venue symbols remain unchanged in Postgres metadata.

    No detector thresholds, target selection, data resolution or retention rules change.
    """

    original_flush: Callable[..., Awaitable[int]] = stream_module.BroadObservationStore.flush

    async def serialized_flush(self: Any, force: bool = False) -> int:
        lock = getattr(self, "_serialized_db_flush_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._serialized_db_flush_lock = lock

        async with lock:
            for attempt in range(1, 5):
                try:
                    return await original_flush(self, force=force)
                except Exception as exc:
                    if not _is_retryable_db_conflict(exc) or attempt >= 4:
                        raise
                    delay = 0.05 * (2 ** (attempt - 1))
                    stream_module.logger.warning(
                        "Broad observation DB conflict retried attempt=%s delay=%.2fs error=%s",
                        attempt,
                        delay,
                        exc,
                    )
                    await asyncio.sleep(delay)
        return 0

    stream_module.BroadObservationStore.flush = serialized_flush
    stream_module.BroadObservationStore._retry_preservations_sync = _safe_retry_preservations_sync
    stream_module.RawSegmentWriter._upload = _safe_raw_segment_upload
