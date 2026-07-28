from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from dateutil import parser as date_parser


@dataclass(slots=True)
class Page:
    rows: list[dict[str, Any]]
    cursor: dict[str, Any]
    done: bool = True


def as_utc(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = date_parser.isoparse(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


class BaseProvider:
    name: str

    def catalogue(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    def iter_bar_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        raise NotImplementedError

    def iter_trade_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        raise NotImplementedError

    def iter_quote_pages(self, partition: dict[str, Any]) -> Iterable[Page]:
        raise NotImplementedError
