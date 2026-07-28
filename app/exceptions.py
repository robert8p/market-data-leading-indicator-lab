from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class ProviderError(Exception):
    message: str
    retryable: bool = True
    retry_at: datetime | None = None
    code: str | None = None

    def __str__(self) -> str:
        return self.message


class EmptyData(Exception):
    pass


class PauseRequested(Exception):
    pass


class CancelRequested(Exception):
    pass
