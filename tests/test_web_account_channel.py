from __future__ import annotations

import urllib.parse
from pathlib import Path

import cfquant_web_server as web


def test_verify_account_pair_defaults_to_trade_channel(monkeypatch) -> None:
    captured: dict[str, str] = {}

    monkeypatch.setattr(web, "bridge_config", lambda bridge_id: {"id": bridge_id})
    monkeypatch.setattr(web.STATUS_MONITOR, "latest", lambda bridge_id: {"bridge_id": bridge_id})

    def fake_get(bridge_id, channel, account_id, sections, force=False):  # noqa: ANN001
        captured.update(
            bridge_id=bridge_id,
            channel=channel,
            account_id=account_id,
        )
        return {"sections": sections, "force": force}

    monkeypatch.setattr(web.ACCOUNT_CACHE, "get", fake_get)

    result = web.verify_account_pair(
        {"account_id": "2070001669", "bridge_id": "default"}
    )

    assert captured == {
        "bridge_id": "default",
        "channel": "trade",
        "account_id": "2070001669",
    }
    assert result["channel"] == "trade"


def test_verify_account_pair_coerces_normal_to_trade(monkeypatch) -> None:
    captured: dict[str, str] = {}

    monkeypatch.setattr(web, "bridge_config", lambda bridge_id: {"id": bridge_id})
    monkeypatch.setattr(web.STATUS_MONITOR, "latest", lambda bridge_id: {"bridge_id": bridge_id})

    def fake_get(bridge_id, channel, account_id, sections, force=False):  # noqa: ANN001
        captured["channel"] = channel
        return {"sections": sections, "force": force}

    monkeypatch.setattr(web.ACCOUNT_CACHE, "get", fake_get)

    result = web.verify_account_pair(
        {
            "account_id": "2070001669",
            "bridge_id": "default",
            "channel": "normal",
        }
    )

    assert captured["channel"] == "trade"
    assert result["channel"] == "trade"


def test_account_api_defaults_to_trade_channel(monkeypatch) -> None:
    captured: dict[str, str] = {}

    monkeypatch.setattr(web, "bridge_config", lambda bridge_id: {"id": bridge_id})

    def fake_get(bridge_id, channel, account_id, sections, force=False):  # noqa: ANN001
        captured.update(
            bridge_id=bridge_id,
            channel=channel,
            account_id=account_id,
        )
        return {"sections": sections, "force": force}

    monkeypatch.setattr(web.ACCOUNT_CACHE, "get", fake_get)

    handler = object.__new__(web.CfquantWebHandler)
    handler._authorized = lambda parsed: True
    handler._write_json = lambda payload, status=200: None
    handler._handle_api_get(
        urllib.parse.urlparse(
            "/api/account?account_id=2070001669&sections=asset,positions"
        )
    )

    assert captured == {
        "bridge_id": "default",
        "channel": "trade",
        "account_id": "2070001669",
    }


def test_account_api_coerces_normal_to_trade(monkeypatch) -> None:
    captured: dict[str, str] = {}

    monkeypatch.setattr(web, "bridge_config", lambda bridge_id: {"id": bridge_id})

    def fake_get(bridge_id, channel, account_id, sections, force=False):  # noqa: ANN001
        captured["channel"] = channel
        return {"sections": sections, "force": force}

    monkeypatch.setattr(web.ACCOUNT_CACHE, "get", fake_get)

    handler = object.__new__(web.CfquantWebHandler)
    handler._authorized = lambda parsed: True
    handler._write_json = lambda payload, status=200: None
    handler._handle_api_get(
        urllib.parse.urlparse(
            "/api/account?account_id=2070001669&channel=normal&sections=asset"
        )
    )

    assert captured["channel"] == "trade"


def test_dashboard_uses_trade_selector_for_account_queries() -> None:
    app_source = (Path(web.STATIC_DIR) / "app.js").read_text(encoding="utf-8")

    verify_block = app_source.split("async function verifyPair", 1)[1].split("async function", 1)[0]
    refresh_block = app_source.split("async function refreshAccount", 1)[1].split("function normalizeStockCode", 1)[0]
    assert "channel: selectedTradeChannel()" in verify_block
    assert "const channel = selectedTradeChannel();" in refresh_block
