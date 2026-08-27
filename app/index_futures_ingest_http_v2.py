from __future__ import annotations

import re
from datetime import timedelta

from app import index_futures_ingest_http as base


def _discover_without_type_lookahead(client: base.MassiveClient, rpc: base.SupabaseRPC, root: str) -> list[dict]:
    """Discover quarterly contracts using only historical fields supported across the full window.

    Identity is enforced using product code, expected exchange MIC, strict quarterly full-contract
    ticker, and trade dates. Provider-return order is irrelevant and is sorted locally afterwards.
    """
    upper = base._END_EXCLUSIVE + timedelta(days=130)
    payload = client.get(
        "/futures/v1/contracts",
        {
            "product_code": root,
            "last_trade_date.gte": base._START.isoformat(),
            "last_trade_date.lte": upper.isoformat(),
            "first_trade_date.lt": base._END_EXCLUSIVE.isoformat(),
            "limit": 1000,
        },
    )
    pattern = re.compile(rf"^{re.escape(root)}[HMUZ][0-9]{{1,2}}$")
    result: list[dict] = []
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
        contract_id = rpc.call(
            "ifv1_upsert_contract",
            {
                "p_root": root,
                "p_asof": (base._END_EXCLUSIVE - timedelta(days=1)).isoformat(),
                "p_payload": row,
            },
        )
        result.append(
            {
                "contract_id": str(contract_id),
                "root": root,
                "ticker": ticker,
                "last_trade_date": last_trade,
                "first_trade_date": base._as_date(row.get("first_trade_date")),
                "settlement_date": base._as_date(row.get("settlement_date")),
            }
        )
    result.sort(key=lambda item: (item["last_trade_date"], item["ticker"]))
    return result


# Patch the discovery routine before the base module's opt-in starter is called.
base._discover = _discover_without_type_lookahead
start_index_futures_ingestion_if_enabled = base.start_index_futures_ingestion_if_enabled
