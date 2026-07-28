from app.providers.alpaca import AlpacaProvider


def _asset(exchange: str):
    return {
        "symbol": "OTCX",
        "name": "OTC Example",
        "exchange": exchange,
        "status": "active",
        "tradable": True,
        "easy_to_borrow": False,
    }


def test_otc_asset_is_catalogued_but_not_planned_without_otc_entitlement(monkeypatch):
    provider = AlpacaProvider()
    provider.settings.alpaca_otc_enabled = False
    monkeypatch.setattr(provider.http, "get", lambda *_args, **_kwargs: [_asset("OTC")])

    row = provider.catalogue()[0]
    assert row["tradable"] is True
    assert row["preferred"] is False
    assert row["source_feed"] == "otc"
    assert row["metadata"]["collection_excluded_reason"] == "otc_subscription_required"


def test_otc_asset_uses_otc_feed_when_enabled(monkeypatch):
    provider = AlpacaProvider()
    provider.settings.alpaca_otc_enabled = True
    monkeypatch.setattr(provider.http, "get", lambda *_args, **_kwargs: [_asset("OTC")])

    row = provider.catalogue()[0]
    assert row["tradable"] is True
    assert row["preferred"] is True
    assert row["source_feed"] == "otc"
