from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cfquant.xttrader import XtQuantTrader
from cfquant.xttype import StockAccount, XtAsset


def _trader_with_asset_result(result):
    account = StockAccount("2070001669", bridge_id="default")
    trader = XtQuantTrader(account=account)
    trader._trade_request = lambda action, params: result
    return trader, account


def test_query_stock_asset_empty_list_returns_none() -> None:
    trader, account = _trader_with_asset_result([])

    assert trader.query_stock_asset(account) is None


def test_query_stock_asset_single_item_list_returns_asset() -> None:
    trader, account = _trader_with_asset_result(
        [{"account_id": "2070001669", "balance": 100000.0, "available": 80000.0}]
    )

    asset = trader.query_stock_asset(account)

    assert isinstance(asset, XtAsset)
    assert asset.account_id == "2070001669"
    assert asset.balance == 100000.0
    assert asset.available == 80000.0


def test_query_stock_asset_multiple_items_raise() -> None:
    trader, account = _trader_with_asset_result(
        [
            {"account_id": "2070001669", "balance": 100000.0},
            {"account_id": "2070001669", "balance": 100001.0},
        ]
    )

    with pytest.raises(RuntimeError, match="expected exactly one account detail.*got 2"):
        trader.query_stock_asset(account)
