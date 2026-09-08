from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cfquant.xttrader import XtQuantTrader
from cfquant.xttype import StockAccount, XtAsset
from cfquant.tx_trade_bridge import TxTradeBridge


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


def test_credit_sdk_query_and_order_params_reach_bridge_offline() -> None:
    account_id = "28160000447"
    observed = []

    def get_trade_detail_data(received_account_id, account_type, detail_type):
        observed.append((received_account_id, account_type, detail_type))
        return [{"m_strAccountID": account_id, "m_dBalance": 5000.0}]

    passorder_calls = []

    def passorder(*args):
        passorder_calls.append(args)
        return 12345

    bridge = TxTradeBridge(
        context=None,
        globals_dict={
            "get_trade_detail_data": get_trade_detail_data,
            "passorder": passorder,
        },
        show=False,
    )
    trader = XtQuantTrader(account=StockAccount(account_id, "CREDIT"))
    trader._trade_request = lambda action, params, timeout=None: bridge._dispatch(action, params, {})

    details = trader.query_credit_detail(trader.account)
    order_id = trader.order_stock(
        trader.account,
        "600000.SH",
        23,
        100,
        11,
        10.1,
        strategy_name="credit-strategy",
        order_remark="credit-sdk-order",
    )

    assert details[0]["account_id"] == account_id
    assert observed == [(account_id, "credit", "account")]
    assert order_id == 12345
    assert passorder_calls[0][0] == 33
    assert passorder_calls[0][2] == account_id
    assert passorder_calls[0][7:10] == ("credit-strategy", 2, "credit-sdk-order")
