from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable


_DEADLOCK_MARKERS = (
    "deadlock detected",
    "deadlock",
    "serialization failure",
)


def _is_retryable_db_conflict(exc: BaseException) -> bool:
    text = str(exc).lower()
    return any(marker in text for marker in _DEADLOCK_MARKERS)


def install_crypto_stream_deadlock_fixes(stream_module: Any) -> None:
    """Prevent concurrent broad-observation flushes from deadlocking PostgreSQL.

    Broad-market trigger preservation can call ``BroadObservationStore.flush(force=True)``
    while the normal aggregate heartbeat is also flushing the same store. Those calls
    used separate database transactions and could lock overlapping upsert rows in a
    different order. The resulting PostgreSQL deadlock bubbled up into the scanner loop,
    forcing otherwise healthy venue connections to reconnect.

    The fix is intentionally narrow: serialize broad-store flushes inside the worker and
    retry a deadlock a few times after the store's existing failure path requeues rows.
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
