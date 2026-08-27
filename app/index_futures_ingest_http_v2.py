from __future__ import annotations

import re
from datetime import timedelta

from app import index_futures_ingest_http as base


_SNAPSHOT_DATES = (
    base._START,
    base._START + timedelta(days=365),
    base._END_EXCLUSIVE - timedelta(days=1),
)


def _discover_point_in_time(client: base.MassiveClient, rpc: base.SupabaseRPC, root: str) -> list[dict]:
    """Reconstruct the quarterly contract set from historical point-in-time snapshots.

    A current reference snapshot is not a reliable catalogue of expired contracts. We query
    three dates spanning the licensed two-year window with active=true, preserve each snapshot
    in metadata history, and deduplicate by the verified full contract ticker.
    """
    pattern = re.compile(rf"^{re.escape(root)}[HMUZ][0-9]{{1,2}}$")
    discovered: dict[str, dict] = {}

    for asof in _SNAPSHOT_DATES:
        payload = client.get(
            "/futures/v1/contracts",
            {
                "date": asof.isoformat(),
                "product_code": root,
                "active": "true",
                "limit": 1000,
                "sort": "ticker.asc",
            },
        )
        for row in payload.get("results", []):
            if not isinstance(row, dict):
                continue
            ticker = str(row.get("ticker") or "")
            last_trade = base._as_date(row.get("last_trade_date"))
            if (
                row.get("product_code") != root
                or row.get("trading_venue") != base._EXPECTED_VENUE[root]
                or not pattern.match(ticker)
                or not last_trade
            ):
                continue
            # Keep contracts that can contribute observations to the target window, plus the
            # immediately following quarter needed to observe the final in-window roll state.
            if last_trade < base._START or last_trade > base._END_EXCLUSIVE + timedelta(days=130):
                continue
            contract_id = rpc.call(
                "ifv1_upsert_contract",
                {"p_root": root, "p_asof": asof.isoformat(), "p_payload": row},
            )
            discovered[ticker] = {
                "contract_id": str(contract_id),
                "root": root,
                "ticker": ticker,
                "last_trade_date": last_trade,
                "first_trade_date": base._as_date(row.get("first_trade_date")),
                "settlement_date": base._as_date(row.get("settlement_date")),
            }

    result = list(discovered.values())
    result.sort(key=lambda item: (item["last_trade_date"], item["ticker"]))
    return result


# Patch the discovery routine before the base module's opt-in starter is called.
base._discover = _discover_point_in_time
start_index_futures_ingestion_if_enabled = base.start_index_futures_ingestion_if_enabled
