from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cfquant.protocol import loads_message
from cfquant.normal_bridge import NormalQmtBridge
from cfquant.tx_trade_bridge import TxTradeBridge


class FakeTx:
    def __init__(self) -> None:
        self.pushed = []
        self.closed = False

    def push(self, kind, payload, client_id) -> None:  # noqa: ANN001
        self.pushed.append((kind, payload, client_id))

    def close(self) -> None:
        self.closed = True


class FakeQuoteContext:
    def __init__(self) -> None:
        self.callback = None
        self.unsubscribe_calls = []

    def subscribe_quote(self, stock_code, period, start_time="", end_time="", count=0, callback=None):  # noqa: ANN001
        self.callback = callback
        return 17

    def unsubscribe_quote(self, subscribe_id):  # noqa: ANN001
        self.unsubscribe_calls.append(subscribe_id)
        return True


class FakeOrderLookupContext:
    def __init__(self, *, passorder_result=None, orders=None) -> None:
        self.passorder_result = passorder_result
        self.orders = list(orders or [])
        self.passorder_calls = []
        self.query_calls = []

    def passorder(self, *args):  # noqa: ANN002
        self.passorder_calls.append(args)
        return self.passorder_result

    def get_trade_detail_data(self, account_id, account_type, detail_type):  # noqa: ANN001
        self.query_calls.append((account_id, account_type, detail_type))
        return list(self.orders)


class FakeCancelContext:
    def __init__(self, *, order_status=50, cancel_result=True) -> None:
        self.order_status = order_status
        self.cancel_result = cancel_result
        self.query_calls = []
        self.cancel_calls = []

    def get_trade_detail_data(self, account_id, account_type, detail_type):  # noqa: ANN001
        self.query_calls.append((account_id, account_type, detail_type))
        return [
            SimpleNamespace(
                m_strOrderSysID="3",
                m_nOrderStatus=self.order_status,
            )
        ]

    def cancel(self, order_id, account_id, account_type, context):  # noqa: ANN001
        self.cancel_calls.append((order_id, account_id, account_type, context))
        return self.cancel_result


def _bridge() -> TxTradeBridge:
    return TxTradeBridge(context=None, globals_dict={})


def test_normal_and_lowlat_bridges_share_order_and_cancel_implementation() -> None:
    assert NormalQmtBridge._order_stock is TxTradeBridge._order_stock
    assert NormalQmtBridge._cancel_order_stock is TxTradeBridge._cancel_order_stock


def test_format_trade_detail_order_exposes_standard_alias_fields() -> None:
    bridge = _bridge()
    raw = SimpleNamespace(
        m_strInstrumentID="600000",
        m_strExchangeID="SH",
        m_strInstrumentName="PF Bank",
        m_strRemark="cid-1",
        m_nOrderID=12345,
        m_strOrderSysID="SYS-1",
        m_nOrderType=23,
        m_nPriceType=42,
        m_dOrderPrice=10.25,
        m_nOrderStatus=56,
        m_strOrderDate="20260715",
        m_strOrderTime="09:35:01",
    )

    payload = bridge._format_trade_detail(raw, "order")

    assert payload["stock_code"] == "600000.SH"
    assert payload["account_id"] == ""
    assert payload["order_remark"] == "cid-1"
    assert payload["order_id"] == 12345
    assert payload["order_sysid"] == "SYS-1"
    assert payload["order_type"] == 23
    assert payload["direction"] == 23
    assert payload["price_type"] == 42
    assert payload["price"] == 10.25
    assert payload["order_status"] == 56
    assert payload["order_date"] == "20260715"
    assert payload["order_time"] == "09:35:01"
    assert payload["m_strOrderSysID"] == "SYS-1"


def test_format_trade_detail_deal_exposes_standard_alias_fields() -> None:
    bridge = _bridge()
    raw = SimpleNamespace(
        m_strInstrumentID="920833",
        m_strExchangeID="BJ",
        m_strInstrumentName="BSE Name",
        m_strRemark="cid-bj",
        m_nOrderID=456,
        m_strOrderSysID="SYS-BJ",
        m_nOrderType=24,
        m_nPriceType=11,
        m_strTradeID="TR-1",
        m_strDealID="DL-1",
        m_dPrice=9.88,
        m_nVolume=300,
        m_nOrderStatus=55,
        m_strTradeDate="20260715",
        m_strTradeTime="10:01:05",
    )

    payload = bridge._format_trade_detail(raw, "deal")

    assert payload["stock_code"] == "920833.BJ"
    assert payload["account_id"] == ""
    assert payload["order_remark"] == "cid-bj"
    assert payload["order_id"] == 456
    assert payload["order_sysid"] == "SYS-BJ"
    assert payload["trade_id"] == "TR-1"
    assert payload["deal_id"] == "DL-1"
    assert payload["order_type"] == 24
    assert payload["direction"] == 24
    assert payload["price_type"] == 11
    assert payload["price"] == 9.88
    assert payload["order_status"] == 55
    assert payload["trade_date"] == "20260715"
    assert payload["trade_time"] == "10:01:05"


def test_format_trade_detail_prefers_stock_offset_flag_for_direction() -> None:
    bridge = _bridge()
    raw = SimpleNamespace(
        m_strInstrumentID="513300",
        m_strExchangeID="SH",
        m_nDirection=48,
        m_nOffsetFlag=49,
    )

    order = bridge._format_trade_detail(raw, "order")
    deal = bridge._format_trade_detail(raw, "deal")

    assert order["direction"] == 49
    assert deal["direction"] == 49


def test_dispatch_subscribe_quote_pushes_quote_event_and_unsubscribe_cleans_up() -> None:
    context = FakeQuoteContext()
    bridge = TxTradeBridge(context=context, globals_dict={})
    bridge.tx = FakeTx()

    result = bridge._dispatch(
        "xtdata.subscribe_quote",
        {
            "stock_code": "600000.SH",
            "period": "tick",
            "start_time": "",
            "end_time": "",
            "count": 0,
        },
        {"client_id": "cli-1", "reply_channel": "cli-1"},
    )

    assert result == {"subscribe_id": 17}
    assert 17 in bridge.subscriptions
    assert bridge.client_subscriptions["cli-1"] == {17}

    assert context.callback is not None
    context.callback({"600000.SH": {"lastPrice": 10.2}})

    kind, payload, client_id = bridge.tx.pushed[-1]
    event = loads_message(payload)
    assert kind == "event"
    assert client_id == "cli-1"
    assert event["type"] == "event"
    assert event["event"] == "quote:17"
    assert event["subscription_id"] == 17
    assert event["data"] == {"600000.SH": {"lastPrice": 10.2}}

    unsubscribe = bridge._dispatch(
        "xtdata.unsubscribe_quote",
        {"subscribe_id": 17},
        {"client_id": "cli-1", "reply_channel": "cli-1"},
    )

    assert unsubscribe is True
    assert context.unsubscribe_calls == [17]
    assert bridge.subscriptions == {}
    assert bridge.client_subscriptions == {}


def test_query_trade_detail_propagates_request_account_id_to_all_payload_types() -> None:
    row = SimpleNamespace(m_strInstrumentID="600000", m_strExchangeID="SH", m_dBalance=100.0, m_nVolume=200)
    bridge = TxTradeBridge(
        context=None,
        globals_dict={"get_trade_detail_data": lambda account_id, account_type, detail_type: [row]},
    )

    order_rows = bridge._query_trade_detail({"account_id": "2070001669"}, "order")
    deal_rows = bridge._query_trade_detail({"account_id": "2070001669"}, "deal")
    position_rows = bridge._query_trade_detail({"account_id": "2070001669"}, "position")
    account_rows = bridge._query_trade_detail({"account_id": "2070001669"}, "account")

    assert order_rows[0]["account_id"] == "2070001669"
    assert deal_rows[0]["account_id"] == "2070001669"
    assert position_rows[0]["account_id"] == "2070001669"
    assert account_rows[0]["account_id"] == "2070001669"


@pytest.mark.parametrize("detail_type", ["position", "order", "deal"])
def test_query_trade_detail_preserves_empty_list_as_success(detail_type) -> None:
    bridge = TxTradeBridge(
        context=None,
        globals_dict={"get_trade_detail_data": lambda account_id, account_type, kind: []},
    )

    assert bridge._query_trade_detail({"account_id": "2070001669"}, detail_type) == []


@pytest.mark.parametrize("detail_type", ["position", "order", "deal"])
def test_query_trade_detail_rejects_none_snapshot(detail_type) -> None:
    bridge = TxTradeBridge(
        context=None,
        globals_dict={"get_trade_detail_data": lambda account_id, account_type, kind: None},
    )

    with pytest.raises(RuntimeError, match="trade detail query returned None"):
        bridge._query_trade_detail({"account_id": "2070001669"}, detail_type)


def test_close_unsubscribes_remaining_quote_subscriptions() -> None:
    context = FakeQuoteContext()
    bridge = TxTradeBridge(context=context, globals_dict={})
    bridge.tx = FakeTx()
    bridge._dispatch(
        "xtdata.subscribe_quote",
        {"stock_code": "600000.SH", "period": "tick"},
        {"client_id": "cli-1", "reply_channel": "cli-1"},
    )

    bridge.close()

    assert context.unsubscribe_calls == [17]
    assert bridge.subscriptions == {}
    assert bridge.client_subscriptions == {}


def test_order_stock_finds_stable_order_id_by_remark_when_passorder_returns_none() -> None:
    context = FakeOrderLookupContext(
        passorder_result=None,
        orders=[
            SimpleNamespace(
                m_strRemark="cid-remark",
                m_nOrderID=12345,
                m_strOrderSysID="SYS-12345",
            )
        ],
    )
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._order_stock(
        {
            "account_id": "2070001669",
            "stock_code": "600000.SH",
            "price_type": 11,
            "price": 10.1,
            "order_volume": 100,
            "strategy_name": "alpha",
            "order_remark": "cid-remark",
            "find_order_wait": 0,
        },
        {"id": "req-1"},
    )

    assert result["request_result"] is None
    assert result["order_id"] == "SYS-12345"
    assert result["order_remark"] == "cid-remark"
    assert context.query_calls == [("2070001669", "stock", "order")]


@pytest.mark.parametrize("passorder_result", [0, -1, "0", "-1", False])
def test_order_stock_finds_stable_order_id_when_passorder_returns_non_order_status(
    passorder_result,
) -> None:
    context = FakeOrderLookupContext(
        passorder_result=passorder_result,
        orders=[
            SimpleNamespace(
                m_strRemark="cid-zero",
                m_nOrderID=None,
                m_strOrderSysID="SYS-ZERO",
            )
        ],
    )
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._order_stock(
        {
            "account_id": "2070001669",
            "stock_code": "513300.SH",
            "price_type": 11,
            "price": 2.5,
            "order_volume": 100,
            "strategy_name": "limit-probe",
            "order_remark": "cid-zero",
            "find_order_wait": 0,
        },
        {"id": "req-zero"},
    )

    assert result["request_result"] == passorder_result
    assert result["order_id"] == "SYS-ZERO"
    assert context.query_calls == [("2070001669", "stock", "order")]


def test_order_stock_keeps_positive_passorder_order_id_without_lookup() -> None:
    context = FakeOrderLookupContext(passorder_result=12345, orders=[])
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._order_stock(
        {
            "account_id": "2070001669",
            "stock_code": "513300.SH",
            "price_type": 11,
            "price": 2.5,
            "order_volume": 100,
            "order_remark": "cid-positive",
        },
        {"id": "req-positive"},
    )

    assert result["order_id"] == 12345
    assert context.query_calls == []


def test_order_stock_async_keeps_missing_order_id_none_when_lookup_fails() -> None:
    context = FakeOrderLookupContext(passorder_result=None, orders=[])
    bridge = TxTradeBridge(context=context, globals_dict={})
    bridge.tx = FakeTx()

    result = bridge._order_stock_async(
        {
            "seq": 9,
            "account_id": "2070001669",
            "stock_code": "600000.SH",
            "price_type": 11,
            "price": 10.1,
            "order_volume": 100,
            "order_remark": "cid-missing",
            "find_order_wait": 0,
        },
        {"id": "req-2", "client_id": "cli-2"},
    )

    assert result["order_id"] is None
    kind, payload, client_id = bridge.tx.pushed[-1]
    event = loads_message(payload)
    assert kind == "event"
    assert client_id == "cli-2"
    assert event["event"] == "trader:on_order_stock_async_response"
    assert event["data"]["order_id"] is None
    assert event["data"]["order_remark"] == "cid-missing"
    assert context.query_calls == [("2070001669", "stock", "order")]


def test_cancel_queries_active_status_before_native_cancel() -> None:
    context = FakeCancelContext(order_status=50)
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._cancel_order_stock(
        {"account_id": "2070001669", "account_type": "STOCK", "order_id": "3"}
    )

    assert result == {"cancel_result": 0, "request_result": True, "order_id": "3"}
    assert context.query_calls == [("2070001669", "stock", "order")]
    assert context.cancel_calls == [("3", "2070001669", "STOCK", context)]


def test_cancel_refuses_terminal_order_without_native_cancel() -> None:
    context = FakeCancelContext(order_status=56)
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._cancel_order_stock(
        {"account_id": "2070001669", "account_type": "STOCK", "order_id": "3"}
    )

    assert result["cancel_result"] == -1
    assert result["reason"] == "order_not_active_or_status_unknown"
    assert result["order_status"] == 56
    assert context.cancel_calls == []


def test_cancel_refuses_missing_order_without_native_cancel() -> None:
    context = FakeCancelContext(order_status=50)
    context.get_trade_detail_data = lambda *args: []
    bridge = TxTradeBridge(context=context, globals_dict={})

    result = bridge._cancel_order_stock(
        {"account_id": "2070001669", "account_type": "STOCK", "order_id": "missing"}
    )

    assert result["cancel_result"] == -1
    assert result["reason"] == "order_not_active_or_status_unknown"
    assert result["order_status"] is None
    assert context.cancel_calls == []
