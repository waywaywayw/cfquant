from __future__ import annotations

import ast
import importlib
import json
import runpy
import sys
import threading
import time
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cfquant import account_routing
from cfquant.client import LTtxRpcClient
from cfquant.normal_bridge import NormalQmtBridge
from cfquant.protocol import loads_message, pack_request
from cfquant.tx_trade_bridge import TxTradeBridge
from cfquant.xttrader import XtQuantTrader
from cfquant.xttype import StockAccount


class FakeTx:
    def __init__(self) -> None:
        self.pushed: list[tuple[str, str, str]] = []
        self.response_ready = threading.Event()
        self.closed = False

    def push(self, kind: str, payload: str, channel: str) -> None:
        self.pushed.append((kind, payload, channel))
        self.response_ready.set()

    def close(self) -> None:
        self.closed = True


class FakeContext:
    def __init__(self, set_account_error=None, run_time_error=None, events=None) -> None:
        self.whole_quote_calls: list[tuple[list[str], object]] = []
        self.quote_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.unsubscribed: list[int] = []
        self.account_calls: list[str] = []
        self.set_account_error = set_account_error
        self.run_time_error = run_time_error
        self.run_time_calls: list[tuple[str, str, str]] = []
        self.events = events if events is not None else []

    def get_full_tick(self, code_list: list[str]) -> dict[str, dict[str, float]]:
        return {code: {"lastPrice": 12.34} for code in code_list}

    def subscribe_whole_quote(self, markets: list[str], callback=None) -> int:
        self.whole_quote_calls.append((markets, callback))
        return 1

    def subscribe_quote(self, *args, **kwargs) -> int:
        self.quote_calls.append((args, kwargs))
        return 42

    def unsubscribe_quote(self, subscribe_id: int) -> bool:
        self.unsubscribed.append(subscribe_id)
        return True

    def set_account(self, account_id: str) -> None:
        self.account_calls.append(account_id)
        self.events.append(("set_account", account_id))
        if self.set_account_error is not None:
            raise self.set_account_error

    def run_time(self, func_name: str, period: str, start_time: str) -> None:
        self.run_time_calls.append((func_name, period, start_time))
        if self.run_time_error is not None:
            raise self.run_time_error


def _stub_context_startup(bridge: NormalQmtBridge, events: list[object]) -> None:
    bridge._start_worker_thread = lambda context: events.append("worker")
    bridge._schedule_timer = lambda: events.append("timer")


def test_set_context_binds_global_account_injected_after_construction() -> None:
    events = []
    globals_dict = {}
    bridge = NormalQmtBridge(
        context=None,
        globals_dict=globals_dict,
        internal_whole_quote_enabled=False,
        show=False,
    )
    context = FakeContext(events=events)
    bridge._log = lambda message: events.append(("log", message))
    _stub_context_startup(bridge, events)
    globals_dict["account"] = " 28100046850 "

    bridge.set_context(context)

    assert context.account_calls == ["28100046850"]
    assert events.index(("set_account", "28100046850")) < events.index("worker")
    success_log = ("log", "normal bridge callback account bound account=28100046850")
    assert events.count(success_log) == 1
    assert events.index(success_log) < events.index("worker")
    assert events.index("worker") < events.index("timer")
    status = bridge._status()
    assert status["context_ready"] is True
    assert status["callback_account_bound"] is True
    assert status["callback_account_id"] == "28100046850"


@pytest.mark.parametrize(
    "account_id, global_account",
    [
        (" 28100046850 ", ""),
        (" 28100046850 ", "28100046850"),
    ],
)
def test_set_context_binds_explicit_or_matching_accounts(account_id, global_account) -> None:
    events = []
    globals_dict = {"account": global_account}
    bridge = NormalQmtBridge(
        context=None,
        account_id=account_id,
        globals_dict=globals_dict,
        internal_whole_quote_enabled=False,
        show=False,
    )
    context = FakeContext(events=events)
    bridge._log = lambda message: None
    _stub_context_startup(bridge, events)

    bridge.set_context(context)

    assert context.account_calls == ["28100046850"]
    assert bridge.account_id == "28100046850"
    assert bridge.callback_account_bound is True
    assert bridge.callback_account_id == "28100046850"


def test_set_context_rejects_conflicting_accounts_without_startup() -> None:
    events = []
    logs = []
    bridge = NormalQmtBridge(
        context=None,
        account_id="configured-account",
        globals_dict={"account": "global-account"},
        internal_whole_quote_enabled=False,
        show=False,
    )
    context = FakeContext(events=events)
    bridge._log = logs.append
    _stub_context_startup(bridge, events)
    bridge.callback_account_bound = True
    bridge.callback_account_id = "old-account"

    with pytest.raises(ValueError, match="callback account conflict"):
        bridge.set_context(context)

    assert context.account_calls == []
    assert events == []
    status = bridge._status()
    assert status["context_ready"] is False
    assert status["callback_account_bound"] is False
    assert status["callback_account_id"] == ""
    assert any("callback account binding conflict" in message for message in logs)


def test_set_context_native_account_error_fails_closed_without_startup() -> None:
    events = []
    logs = []
    error = RuntimeError("native set_account failed")
    bridge = NormalQmtBridge(
        context=None,
        account_id="account-1",
        globals_dict={},
        internal_whole_quote_enabled=False,
        show=False,
    )
    context = FakeContext(set_account_error=error, events=events)
    bridge._log = logs.append
    _stub_context_startup(bridge, events)
    bridge.callback_account_bound = True
    bridge.callback_account_id = "old-account"

    with pytest.raises(RuntimeError, match="native set_account failed"):
        bridge.set_context(context)

    assert context.account_calls == ["account-1"]
    assert events == [("set_account", "account-1")]
    status = bridge._status()
    assert status["context_ready"] is False
    assert status["callback_account_bound"] is False
    assert status["callback_account_id"] == ""
    assert not any(message.startswith("normal bridge callback account bound") for message in logs)


def test_set_context_without_account_keeps_market_data_mode() -> None:
    events = []
    logs = []
    bridge = NormalQmtBridge(context=None, globals_dict={}, show=False)
    context = FakeContext(events=events)
    bridge._log = logs.append
    _stub_context_startup(bridge, events)

    bridge.set_context(context)

    assert context.account_calls == []
    assert len(context.whole_quote_calls) == 1
    assert events == ["worker", "timer"]
    status = bridge._status()
    assert status["context_ready"] is True
    assert status["callback_account_bound"] is False
    assert status["callback_account_id"] == ""
    assert "normal bridge callback account not bound" in logs


def test_close_clears_normal_callback_account_binding_status() -> None:
    events = []
    bridge = NormalQmtBridge(
        context=None,
        account_id="account-1",
        globals_dict={},
        internal_whole_quote_enabled=False,
        show=False,
    )
    context = FakeContext(events=events)
    bridge._log = lambda message: None
    _stub_context_startup(bridge, events)

    bridge.set_context(context)
    assert bridge.callback_account_bound is True

    bridge.close()

    status = bridge._status()
    assert status["callback_account_bound"] is False
    assert status["callback_account_id"] == ""


def test_queued_request_wakes_worker_immediately() -> None:
    bridge = NormalQmtBridge(context=None, globals_dict={})

    bridge._handle_raw_from_thread(
        pack_request(
            "xtdata.get_full_tick",
            {"code_list": ["603458.SH"]},
            client_id="client-1",
            request_id="request-1",
        )
    )

    assert bridge.request_queue.qsize() == 1
    assert bridge.worker_event.is_set()
    assert bridge.worker_source == "request"


def test_queued_request_is_processed_without_qmt_callback() -> None:
    context = FakeContext()
    bridge = NormalQmtBridge(context=context, globals_dict={})
    bridge.running = True
    bridge.tx = FakeTx()
    bridge._start_worker_thread(context)

    try:
        bridge._handle_raw_from_thread(
            pack_request(
                "xtdata.get_full_tick",
                {"code_list": ["603458.SH"]},
                client_id="client-1",
                request_id="request-1",
            )
        )

        assert bridge.tx.response_ready.wait(1)
        kind, payload, channel = bridge.tx.pushed[-1]
        response = loads_message(payload)
        assert kind == "response"
        assert channel == "client-1"
        assert response["ok"] is True
        assert response["result"] == {"603458.SH": {"lastPrice": 12.34}}
    finally:
        bridge.close()


def test_worker_continues_until_request_backlog_is_empty() -> None:
    context = FakeContext()
    bridge = NormalQmtBridge(
        context=context,
        globals_dict={},
        pump_max_count=1,
    )
    bridge.running = True
    bridge.tx = FakeTx()
    bridge._start_worker_thread(context)

    try:
        for index in range(3):
            bridge._handle_raw_from_thread(
                pack_request(
                    "xtdata.get_full_tick",
                    {"code_list": ["603458.SH"]},
                    client_id="client-1",
                    request_id="request-%s" % index,
                )
            )

        deadline = time.monotonic() + 1
        while len(bridge.tx.pushed) < 3 and time.monotonic() < deadline:
            time.sleep(0.01)

        assert bridge.request_queue.empty()
        assert len(bridge.tx.pushed) == 3
        response_ids = {
            loads_message(payload)["id"]
            for kind, payload, channel in bridge.tx.pushed
            if kind == "response" and channel == "client-1"
        }
        assert response_ids == {"request-0", "request-1", "request-2"}
    finally:
        bridge.close()


def test_direct_quote_mode_avoids_whole_market_subscription(monkeypatch) -> None:
    monkeypatch.setenv("CFQUANT_INTERNAL_WHOLE_QUOTE", "0")
    context = FakeContext()
    bridge = NormalQmtBridge(context=context, globals_dict={})

    bridge._subscribe_internal_whole_quote()
    result = bridge._dispatch(
        "xtdata.subscribe_quote",
        {
            "stock_code": "000001.SZ",
            "period": "tick",
            "start_time": "",
            "end_time": "",
            "count": 0,
        },
        {"client_id": "client-1"},
    )

    assert bridge.internal_whole_quote_enabled is False
    assert context.whole_quote_calls == []
    assert result == {"subscribe_id": 42}
    assert len(context.quote_calls) == 1


def test_default_mode_preserves_whole_market_subscription(monkeypatch) -> None:
    monkeypatch.delenv("CFQUANT_INTERNAL_WHOLE_QUOTE", raising=False)
    context = FakeContext()
    bridge = NormalQmtBridge(context=context, globals_dict={})

    bridge._subscribe_internal_whole_quote()

    assert bridge.internal_whole_quote_enabled is True
    assert len(context.whole_quote_calls) == 1


def test_publish_callback_event_forwards_native_order_fields() -> None:
    bridge = NormalQmtBridge(context=None, globals_dict={})
    bridge.tx = FakeTx()

    bridge.publish_callback_event(
        "trader:on_stock_order",
        SimpleNamespace(
            m_strAccountID="test",
            m_strInstrumentID="301559",
            m_strExchangeID="SZ",
            m_strRemark="WQ-order-remark",
            m_nOrderID=34944,
            m_strOrderSysID="SYS-34944",
            m_nOffsetFlag=23,
            m_nOrderPriceType=50,
            m_dLimitPrice=13.1,
            m_nOrderStatus=54,
            m_nVolumeTotalOriginal=200,
            m_nVolumeTraded=0,
            m_strInsertDate="20260907",
            m_strInsertTime="093010",
        ),
    )

    kind, payload, channel = bridge.tx.pushed[-1]
    event = json.loads(payload)
    data = event["data"]
    assert kind == "event"
    assert channel == bridge.callback_event_channel
    assert event["event"] == "trader:on_stock_order"
    assert event["account_id"] == "test"
    assert data["m_nOffsetFlag"] == 23
    assert data["m_nOrderPriceType"] == 50
    assert data["m_dLimitPrice"] == 13.1
    assert data["m_strInsertDate"] == "20260907"
    assert data["m_strInsertTime"] == "093010"
    assert data["m_nVolumeTraded"] == 0


def test_callback_publisher_reuses_trade_transport_and_account_routing() -> None:
    trade_bridge = TxTradeBridge(
        context=None,
        bridge_id="zs_qmt2",
        globals_dict={"account": "28100046850"},
        show=False,
    )
    trade_bridge.tx = FakeTx()
    trade_bridge.account_subscribers["28100046850"] = {"client-1"}

    publisher = NormalQmtBridge.for_callback_publisher(
        trade_bridge,
        callback_event_channel="cfquant.zs_qmt2.callback.event",
    )
    context = FakeContext()
    publisher._log = lambda message: None
    publisher.bind_callback_account(context)
    publisher.publish_callback_event(
        "trader:on_stock_trade",
        SimpleNamespace(
            m_strAccountID="28100046850",
            m_strInstrumentID="603458",
            m_strExchangeID="SH",
            m_nVolume=100,
        ),
    )

    assert publisher.tx is trade_bridge.tx
    assert context.account_calls == ["28100046850"]
    callback_events = [
        (payload, channel)
        for kind, payload, channel in trade_bridge.tx.pushed
        if kind == "event" and channel == "cfquant.zs_qmt2.callback.event"
    ]
    assert len(callback_events) == 1
    callback_payload, callback_channel = callback_events[0]
    callback_event = json.loads(callback_payload)
    assert callback_channel == "cfquant.zs_qmt2.callback.event"
    assert callback_event["event"] == "trader:on_stock_trade"
    assert callback_event["account_id"] == "28100046850"
    assert callback_event["bridge_id"] == "zs_qmt2"

    routed_events = [
        loads_message(payload)
        for kind, payload, channel in trade_bridge.tx.pushed
        if kind == "event" and channel == "client-1"
    ]
    assert len(routed_events) == 1
    assert routed_events[0]["event"] == "trader:on_stock_trade"


def _locked_credit_trade_bridge(bridge_id="zs_qmt2_credit") -> TxTradeBridge:
    bridge = TxTradeBridge(
        context=None,
        bridge_id=bridge_id,
        account_id="28160000447",
        account_locked=True,
        account_type="CREDIT",
        globals_dict={},
        show=False,
    )
    bridge.tx = FakeTx()
    return bridge


def test_locked_publisher_factory_rejects_bridge_and_account_conflicts_only_when_locked() -> None:
    locked_trade = _locked_credit_trade_bridge()

    with pytest.raises(ValueError, match="bridge_id"):
        NormalQmtBridge.for_callback_publisher(locked_trade, bridge_id="other-bridge")
    assert locked_trade.callback_publisher is None

    with pytest.raises(ValueError, match="account_id"):
        NormalQmtBridge.for_callback_publisher(locked_trade, account_id="other-account")
    assert locked_trade.callback_publisher is None

    ordinary_trade = TxTradeBridge(
        context=None,
        bridge_id="ordinary-trade",
        account_id="ordinary-account",
        globals_dict={},
        show=False,
    )
    ordinary_trade.tx = FakeTx()
    publisher = NormalQmtBridge.for_callback_publisher(
        ordinary_trade,
        bridge_id="other-bridge",
        account_id="other-account",
    )
    assert publisher.bridge_id == "other-bridge"
    assert publisher.account_id == "other-account"
    assert publisher.tx is ordinary_trade.tx


def test_locked_publisher_status_and_shared_transport_lifecycle() -> None:
    trade_bridge = _locked_credit_trade_bridge()
    context = FakeContext()
    trade_bridge.set_context(context)
    publisher = NormalQmtBridge.for_callback_publisher(
        trade_bridge,
        callback_event_channel="credit.callback.event",
    )
    publisher.bind_callback_account(context)

    status = trade_bridge._status()
    assert status["context_ready"] is True
    assert status["tx_ready"] is True
    assert status["callback_account_bound"] is True
    assert status["callback_account_id"] == "28160000447"
    assert status["callback_source"] == "trade_model"

    shared_tx = trade_bridge.tx
    publisher.close()
    assert publisher.tx is None
    assert trade_bridge.tx is shared_tx
    assert shared_tx.closed is False
    assert trade_bridge._status()["callback_account_bound"] is False

    publisher = NormalQmtBridge.for_callback_publisher(
        trade_bridge,
        callback_event_channel="credit.callback.event",
    )
    publisher.bind_callback_account(context)
    trade_bridge.close()
    assert trade_bridge.callback_publisher is None
    assert trade_bridge.tx is None
    assert publisher.tx is None
    assert shared_tx.closed is True


def test_locked_publisher_drops_wrong_or_conflicting_identity_and_supplements_only_after_bind() -> None:
    trade_bridge = _locked_credit_trade_bridge("credit-publisher-identity")
    publisher = NormalQmtBridge.for_callback_publisher(trade_bridge)
    publisher.bind_callback_account(FakeContext())
    trade_bridge.account_subscribers["28160000447"] = {"credit-client"}

    def pushed_events():
        return [item for item in trade_bridge.tx.pushed if item[0] == "event"]

    publisher.publish_callback_event(
        "trader:on_stock_order",
        SimpleNamespace(m_strAccountID="other-account", m_nOrderType=27),
    )
    publisher.publish_callback_event(
        "trader:on_stock_order",
        SimpleNamespace(account_id="28160000447", m_strAccountID="other-account"),
    )
    assert pushed_events() == []

    publisher.publish_callback_event(
        "trader:on_stock_trade",
        SimpleNamespace(
            m_nOrderType=27,
            m_strTradeID="TRADE-1",
            m_strDealID="DEAL-1",
            m_strTradeTime="09:36:01",
            m_strTradeDate="20260908",
            m_nVolume=100,
            m_dTradeAmount=1234.5,
        ),
    )
    events = pushed_events()
    assert len(events) == 2
    broadcast = json.loads(next(payload for kind, payload, channel in events if channel == publisher.callback_event_channel))
    assert broadcast["account_id"] == "28160000447"
    assert broadcast["data"]["account_id"] == "28160000447"
    assert broadcast["data"]["m_nOrderType"] == 27
    assert broadcast["data"]["m_strTradeID"] == "TRADE-1"
    assert broadcast["data"]["m_strDealID"] == "DEAL-1"
    assert broadcast["data"]["m_strTradeTime"] == "09:36:01"
    assert broadcast["data"]["m_strTradeDate"] == "20260908"

    direct = loads_message(next(payload for kind, payload, channel in events if channel == "credit-client"))
    callback = []

    class Callback:
        def on_stock_trade(self, trade):
            callback.append(trade)

    trader = XtQuantTrader(callback=Callback(), account=StockAccount("28160000447", "CREDIT"))
    trader._make_trader_handler("on_stock_trade")(direct["data"])
    assert len(callback) == 1
    assert callback[0].m_strTradeID == "TRADE-1"
    assert callback[0].m_strDealID == "DEAL-1"
    assert callback[0].m_strTradeTime == "09:36:01"
    assert callback[0].m_strTradeDate == "20260908"


def test_locked_callback_publisher_isolates_credit_events_between_two_clients() -> None:
    normal_account = "NORMAL-CLIENT-1"
    credit_account = "CREDIT-CLIENT-2"
    normal_bridge_id = "normal-callback-isolation"
    credit_bridge_id = "credit-callback-isolation"
    normal_events = []
    credit_events = []
    normal_context = FakeContext()
    credit_context = FakeContext()
    normal_bridge = TxTradeBridge(
        context=normal_context,
        bridge_id=normal_bridge_id,
        account_id=normal_account,
        globals_dict={},
        show=False,
    )
    credit_bridge = TxTradeBridge(
        context=credit_context,
        bridge_id=credit_bridge_id,
        account_id=credit_account,
        account_locked=True,
        account_type="CREDIT",
        globals_dict={},
        show=False,
    )
    normal_bridge.tx = FakeTx()
    credit_bridge.tx = FakeTx()
    credit_publisher = NormalQmtBridge.for_callback_publisher(
        credit_bridge,
        callback_event_channel="credit.callback.event",
    )
    credit_publisher.bind_callback_account(credit_context)

    class Callback:
        def __init__(self, events) -> None:
            self.events = events

        def on_stock_order(self, order) -> None:  # noqa: ANN001
            self.events.append(("order", order))

        def on_stock_trade(self, trade) -> None:  # noqa: ANN001
            self.events.append(("trade", trade))

    normal_trader = XtQuantTrader(
        callback=Callback(normal_events),
        account=StockAccount(normal_account, "STOCK", bridge_id=normal_bridge_id),
    )
    credit_trader = XtQuantTrader(
        callback=Callback(credit_events),
        account=StockAccount(credit_account, "CREDIT", bridge_id=credit_bridge_id),
    )
    clients = []

    def attach_client(trader, bridge):  # noqa: ANN001
        client = LTtxRpcClient(
            host="127.0.0.1",
            port=2049,
            token="LTtx",
            request_channel=bridge.request_channel,
            client_id=trader.client_id,
        )

        def request(action, params=None, timeout=None):  # noqa: ANN001
            return bridge._dispatch(
                action,
                params or {},
                {"client_id": client.client_id, "reply_channel": client.client_id},
            )

        client.request = request
        trader._clients[bridge.bridge_id] = client
        trader._client = client
        trader._register_trader_events(bridge.bridge_id)
        trader.subscribe(trader.account)
        clients.append(client)
        return client

    def dispatch_for_client(tx, client):  # noqa: ANN001
        dispatched = []
        for kind, payload, channel in tx.pushed:
            if kind != "event" or channel != client.client_id:
                continue
            message = loads_message(payload)
            assert message is not None
            dispatched.append(message)
            client._dispatch_event(message)
        return dispatched

    normal_client = None
    credit_client = None
    try:
        normal_client = attach_client(normal_trader, normal_bridge)
        credit_client = attach_client(credit_trader, credit_bridge)
        assert account_routing.client_ids(normal_bridge_id, normal_account) == [normal_client.client_id]
        assert account_routing.client_ids(credit_bridge_id, credit_account) == [credit_client.client_id]
        normal_route_before = dict(normal_bridge.account_subscribers)
        normal_tx_before = list(normal_bridge.tx.pushed)

        credit_publisher.publish_callback_event(
            "trader:on_stock_order",
            SimpleNamespace(m_nOrderType=27),
        )
        assert credit_bridge.tx.pushed
        assert credit_bridge.tx.pushed[-1][2] == credit_client.client_id
        supplemental = dispatch_for_client(credit_bridge.tx, credit_client)
        assert len(supplemental) == 1
        assert supplemental[0]["data"]["account_id"] == credit_account
        assert len(credit_events) == 1
        assert credit_events[0][0] == "order"
        assert credit_events[0][1].m_nOrderType == 27

        credit_events[:] = []
        credit_bridge.tx.pushed[:] = []
        credit_publisher.publish_callback_event(
            "trader:on_stock_order",
            SimpleNamespace(m_strAccountID=normal_account, m_nOrderType=27),
        )
        credit_publisher.publish_callback_event(
            "trader:on_stock_order",
            SimpleNamespace(
                account_id=credit_account,
                m_strAccountID="CONFLICTING-ACCOUNT",
                m_nOrderType=27,
            ),
        )
        assert credit_bridge.tx.pushed == []
        assert normal_bridge.tx.pushed == normal_tx_before

        credit_bridge.tx.pushed[:] = []
        credit_publisher.publish_callback_event(
            "trader:on_stock_order",
            SimpleNamespace(
                m_strAccountID=credit_account,
                m_nOrderType=27,
                m_nOrderID=7001,
                m_strOrderID="ORDER-7001",
                m_strOrderSysID="SYS-7001",
                m_strInsertDate="20260908",
                m_strInsertTime="09:35:01",
            ),
        )
        credit_publisher.publish_callback_event(
            "trader:on_stock_trade",
            SimpleNamespace(
                m_strAccountID=credit_account,
                m_nOperationType=28,
                m_nTradeID=8001,
                m_nDealID=9001,
                m_strTradeID="TRADE-8001",
                m_strDealID="DEAL-9001",
                m_strTradeDate="20260908",
                m_strTradeTime="09:36:01",
            ),
        )
        direct_messages = dispatch_for_client(credit_bridge.tx, credit_client)
        broadcast_messages = [
            json.loads(payload)
            for kind, payload, channel in credit_bridge.tx.pushed
            if kind == "event" and channel == credit_publisher.callback_event_channel
        ]
        assert len(broadcast_messages) == 2
        assert {message["account_id"] for message in broadcast_messages} == {credit_account}
        assert [message["event"] for message in direct_messages] == [
            "trader:on_stock_order",
            "trader:on_stock_trade",
        ]
        assert [kind for kind, _event in credit_events] == ["order", "trade"]
        assert normal_events == []
        order = credit_events[0][1]
        trade = credit_events[1][1]
        assert order.m_nOrderType == 27
        assert order.m_nOrderID == 7001
        assert order.m_strOrderID == "ORDER-7001"
        assert order.m_strOrderSysID == "SYS-7001"
        assert order.m_strInsertDate == "20260908"
        assert order.m_strInsertTime == "09:35:01"
        assert trade.m_nOperationType == 28
        assert trade.m_nTradeID == 8001
        assert trade.m_nDealID == 9001
        assert trade.m_strTradeID == "TRADE-8001"
        assert trade.m_strDealID == "DEAL-9001"
        assert trade.m_strTradeDate == "20260908"
        assert trade.m_strTradeTime == "09:36:01"
        dispatch_for_client(normal_bridge.tx, normal_client)
        assert normal_bridge.account_subscribers == normal_route_before
        assert account_routing.client_ids(normal_bridge_id, normal_account) == [normal_client.client_id]

        unbound_bridge = TxTradeBridge(
            context=None,
            bridge_id="credit-unbound-callback",
            account_id="UNBOUND-CREDIT",
            account_locked=True,
            account_type="CREDIT",
            globals_dict={},
            show=False,
        )
        unbound_bridge.tx = FakeTx()
        unbound_publisher = NormalQmtBridge.for_callback_publisher(unbound_bridge)
        try:
            assert unbound_bridge._status()["callback_account_bound"] is False
            unbound_publisher.publish_callback_event(
                "trader:on_stock_trade",
                SimpleNamespace(m_nOperationType=28, m_strTradeID="UNBOUND-TRADE"),
            )
            assert unbound_bridge.tx.pushed == []

            with pytest.raises(RuntimeError, match="native set_account failed"):
                unbound_publisher.bind_callback_account(
                    FakeContext(set_account_error=RuntimeError("native set_account failed"))
                )
            assert unbound_bridge._status()["callback_account_bound"] is False
            unbound_publisher.publish_callback_event(
                "trader:on_stock_trade",
                SimpleNamespace(m_nOperationType=28, m_strTradeID="FAILED-BIND-TRADE"),
            )
            assert unbound_bridge.tx.pushed == []
        finally:
            unbound_publisher.close()
            unbound_bridge.close()
            account_routing.unsubscribe(unbound_bridge.bridge_id)
    finally:
        for trader, bridge in ((normal_trader, normal_bridge), (credit_trader, credit_bridge)):
            try:
                trader.unsubscribe(trader.account)
            except Exception:
                pass
            account_routing.unsubscribe(bridge.bridge_id)
            bridge.account_subscribers.clear()
            bridge.close()
        for client in clients:
            client.close()


def test_unlocked_publisher_keeps_first_callback_identity_semantics() -> None:
    trade_bridge = TxTradeBridge(
        context=None,
        bridge_id="ordinary-callback-identity",
        account_id="ordinary-default",
        globals_dict={},
        show=False,
    )
    trade_bridge.tx = FakeTx()
    publisher = NormalQmtBridge.for_callback_publisher(trade_bridge)

    publisher.publish_callback_event(
        "trader:on_stock_order",
        SimpleNamespace(account_id="first-account", m_strAccountID="second-account"),
    )

    event = json.loads(trade_bridge.tx.pushed[-1][1])
    assert event["account_id"] == "first-account"


def _entry_test_stubs(monkeypatch, calls, published, trade_bridge):
    class StubCallbackBridge:
        @classmethod
        def for_callback_publisher(cls, source_bridge, **kwargs):
            calls.append(("publisher", source_bridge, kwargs))
            publisher = cls()
            source_bridge.callback_publisher = publisher
            return publisher

        def bind_callback_account(self, context):
            calls.append("bind_callback_account")

        def publish_callback_event(self, event_name, obj):
            published.append((event_name, obj))

    def start_tx_trade_bridge(*args, **kwargs):
        calls.append(("starter", kwargs))
        return trade_bridge

    package = types.ModuleType("cfquant.cfquant")
    package.__path__ = []
    tx_module = types.ModuleType("cfquant.cfquant.tx_trade_bridge")
    tx_module.start_tx_trade_bridge = start_tx_trade_bridge
    channels_module = types.ModuleType("cfquant.cfquant.channels")
    channels_module.channels_for_bridge = lambda bridge_id: {
        "normal": "normal.request",
        "trade": "trade.request",
        "callback": "callback.event",
    }
    channels_module.normalize_bridge_id = lambda bridge_id: bridge_id
    normal_module = types.ModuleType("cfquant.cfquant.normal_bridge")
    normal_module.NormalQmtBridge = StubCallbackBridge
    monkeypatch.setitem(sys.modules, "cfquant.cfquant", package)
    monkeypatch.setitem(sys.modules, "cfquant.cfquant.tx_trade_bridge", tx_module)
    monkeypatch.setitem(sys.modules, "cfquant.cfquant.channels", channels_module)
    monkeypatch.setitem(sys.modules, "cfquant.cfquant.normal_bridge", normal_module)


def _run_entry_with_constants(monkeypatch, replacements):
    entry_path = Path(__file__).resolve().parents[1] / "qmt_scripts" / "CFQUANT_TRADE_LOWLAT.py"
    source = entry_path.read_text(encoding="ascii")
    for old, new in replacements:
        assert old in source
        source = source.replace(old, new, 1)
    calls = []
    published = []
    trade_bridge = SimpleNamespace(
        context=None,
        ip="127.0.0.1",
        port=2049,
        token="LTtx",
        request_channel="trade.request",
        bridge_id="zs_qmt2_credit",
        account_id="28160000447",
        show=False,
        globals_dict={},
        tx=object(),
    )

    def set_context(context):
        calls.append("set_context")
        trade_bridge.context = context

    def start():
        calls.append("start")
        return trade_bridge

    def poll(max_messages=100, timeout=0):
        calls.append(("poll", max_messages, timeout))

    trade_bridge.set_context = set_context
    trade_bridge.start = start
    trade_bridge.poll = poll

    def close():
        calls.append("close")
        trade_bridge.callback_publisher = None

    trade_bridge.close = close
    _entry_test_stubs(monkeypatch, calls, published, trade_bridge)
    namespace = {}
    exec(compile(source, str(entry_path), "exec"), namespace)
    return namespace, calls, published, trade_bridge


def test_lowlat_locked_entry_uses_cooperative_timer_and_keeps_init_order(monkeypatch) -> None:
    monkeypatch.setenv("CFQUANT_BRIDGE_ID", "ordinary-process-bridge")
    namespace, calls, published, trade_bridge = _run_entry_with_constants(
        monkeypatch,
        [
            ('DEFAULT_ACCOUNT_ID = ""', 'DEFAULT_ACCOUNT_ID = "28160000447"'),
            ('ACCOUNT_TYPE = "STOCK"', 'ACCOUNT_TYPE = "CREDIT"'),
            ("ACCOUNT_LOCKED = False", "ACCOUNT_LOCKED = True"),
            ('USER_BRIDGE_ID = "default"', 'USER_BRIDGE_ID = "zs_qmt2_credit"'),
        ],
    )

    context = FakeContext()
    namespace["init"](context)
    namespace["deal_callback"](context, SimpleNamespace(m_strAccountID="28160000447"))

    starter = next(item for item in calls if isinstance(item, tuple) and item[0] == "starter")
    assert starter[1]["bridge_id"] == "zs_qmt2_credit"
    assert starter[1]["account_id"] == "28160000447"
    assert starter[1]["account_locked"] is True
    assert starter[1]["account_type"] == "CREDIT"
    assert calls.index("set_context") < calls.index("start")
    publisher_index = next(index for index, item in enumerate(calls) if isinstance(item, tuple) and item[0] == "publisher")
    assert calls.index("start") < publisher_index < calls.index("bind_callback_account")
    assert context.run_time_calls == [
        (
            namespace["_RUN_TIME_CALLBACK_NAME"],
            "50nMilliSecond",
            "2019-01-01 00:00:00",
        )
    ]
    assert ("run_forever", 0.001) not in calls
    namespace[namespace["_RUN_TIME_CALLBACK_NAME"]](context)
    namespace["handlebar"](context)
    assert calls[-2:] == [("poll", 100, 0), ("poll", 100, 0)]
    assert len(published) == 1
    assert published[0][0] == "trader:on_stock_trade"
    assert trade_bridge.context is context


def test_lowlat_unlocked_entry_supports_legacy_starter_signature(monkeypatch) -> None:
    entry_path = Path(__file__).resolve().parents[1] / "qmt_scripts" / "CFQUANT_TRADE_LOWLAT.py"
    calls = []

    def legacy_start_tx_trade_bridge(
        context,
        ip="127.0.0.1",
        port=2049,
        token="LTtx",
        request_channel="cfquant.request",
        bridge_id="default",
        account_id="",
        show=True,
    ):
        calls.append(
            (context, ip, port, token, request_channel, bridge_id, account_id, show)
        )
        return object()

    _install_entry_modules(
        monkeypatch,
        tx_starter=legacy_start_tx_trade_bridge,
        callback_bridge=object,
    )
    monkeypatch.setenv("CFQUANT_BRIDGE_ID", "legacy-ordinary")

    exec(compile(entry_path.read_text(encoding="ascii"), str(entry_path), "exec"), {})

    assert calls == [
        (None, "127.0.0.1", 2049, "LTtx", "trade.request", "legacy-ordinary", "", True)
    ]


def test_lowlat_entry_timer_registration_failure_closes_bridge_and_publisher(monkeypatch) -> None:
    namespace, calls, published, trade_bridge = _run_entry_with_constants(monkeypatch, [])

    class MissingRunTimeContext:
        pass

    with pytest.raises(RuntimeError, match="ContextInfo.run_time"):
        namespace["init"](MissingRunTimeContext())

    assert calls[-1] == "close"
    assert namespace["_trade_bridge"] is None
    assert namespace["_callback_bridge"] is None
    assert trade_bridge.callback_publisher is None
    timer_callback = namespace[namespace["_RUN_TIME_CALLBACK_NAME"]]
    timer_callback(MissingRunTimeContext())
    namespace["handlebar"](MissingRunTimeContext())
    assert ("poll", 100, 0) not in calls


def test_lowlat_entry_timer_exception_closes_bridge_and_clears_publisher(monkeypatch) -> None:
    namespace, calls, published, trade_bridge = _run_entry_with_constants(monkeypatch, [])
    context = FakeContext(run_time_error=RuntimeError("timer registration unavailable"))

    with pytest.raises(RuntimeError, match="run_time registration failed"):
        namespace["init"](context)

    assert context.run_time_calls == [
        (
            namespace["_RUN_TIME_CALLBACK_NAME"],
            namespace["_RUN_TIME_PERIOD"],
            namespace["_RUN_TIME_START_TIME"],
        )
    ]
    assert calls[-1] == "close"
    assert namespace["_trade_bridge"] is None
    assert namespace["_callback_bridge"] is None
    assert trade_bridge.callback_publisher is None


def test_lowlat_locked_entry_rejects_invalid_identity_before_starter(monkeypatch) -> None:
    entry_path = Path(__file__).resolve().parents[1] / "qmt_scripts" / "CFQUANT_TRADE_LOWLAT.py"
    source = entry_path.read_text(encoding="ascii")
    source = source.replace('DEFAULT_ACCOUNT_ID = ""', 'DEFAULT_ACCOUNT_ID = "28160000447"', 1)
    source = source.replace("ACCOUNT_LOCKED = False", "ACCOUNT_LOCKED = True", 1)
    source = source.replace('USER_BRIDGE_ID = "default"', 'USER_BRIDGE_ID = ""', 1)
    calls = []

    def forbidden_starter(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("starter must not run for invalid locked configuration")

    package = types.ModuleType("cfquant.cfquant")
    package.__path__ = []
    tx_module = types.ModuleType("cfquant.cfquant.tx_trade_bridge")
    tx_module.start_tx_trade_bridge = forbidden_starter
    monkeypatch.setitem(sys.modules, "cfquant.cfquant", package)
    monkeypatch.setitem(sys.modules, "cfquant.cfquant.tx_trade_bridge", tx_module)

    with pytest.raises(ValueError, match="non-default USER_BRIDGE_ID"):
        exec(compile(source, str(entry_path), "exec"), {})
    assert calls == []


def test_lowlat_entry_has_python36_callbacks_and_publishes_deal_once(monkeypatch) -> None:
    entry_path = Path(__file__).resolve().parents[1] / "qmt_scripts" / "CFQUANT_TRADE_LOWLAT.py"
    source = entry_path.read_text(encoding="ascii")
    tree = ast.parse(source, filename=str(entry_path), feature_version=(3, 6))
    function_names = {
        node.name
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }
    assert {"account_callback", "order_callback", "deal_callback", "position_callback"} <= function_names

    calls = []
    published = []

    class StubTradeBridge:
        def __init__(self):
            self.context = None
            self.ip = "127.0.0.1"
            self.port = 2049
            self.token = "LTtx"
            self.request_channel = "trade.request"
            self.bridge_id = "zs_qmt2"
            self.account_id = ""
            self.show = False
            self.globals_dict = {}
            self.tx = object()
            self.poll_calls = []

        def set_context(self, context):
            calls.append("set_context")
            self.context = context

        def start(self):
            calls.append("start")
            return self

        def poll(self, max_messages=100, timeout=0):
            self.poll_calls.append((max_messages, timeout))

        def close(self):
            calls.append("close")

    trade_bridge = StubTradeBridge()

    def start_tx_trade_bridge(*args, **kwargs):
        calls.append(("starter", kwargs["request_channel"]))
        return trade_bridge

    class StubCallbackBridge:
        @classmethod
        def for_callback_publisher(cls, source_bridge, **kwargs):
            calls.append(("publisher", source_bridge, kwargs["callback_event_channel"]))
            return cls()

        def bind_callback_account(self, context):
            calls.append("bind_callback_account")

        def publish_callback_event(self, event_name, obj):
            published.append((event_name, obj))

    package = types.ModuleType("cfquant.cfquant")
    package.__path__ = []
    tx_module = types.ModuleType("cfquant.cfquant.tx_trade_bridge")
    tx_module.start_tx_trade_bridge = start_tx_trade_bridge
    channels_module = types.ModuleType("cfquant.cfquant.channels")
    channels_module.channels_for_bridge = lambda bridge_id: {
        "normal": "normal.request",
        "trade": "trade.request",
        "callback": "callback.event",
    }
    channels_module.normalize_bridge_id = lambda bridge_id: bridge_id
    normal_module = types.ModuleType("cfquant.cfquant.normal_bridge")
    normal_module.NormalQmtBridge = StubCallbackBridge
    monkeypatch.setitem(sys.modules, "cfquant.cfquant", package)
    monkeypatch.setitem(sys.modules, "cfquant.cfquant.tx_trade_bridge", tx_module)
    monkeypatch.setitem(sys.modules, "cfquant.cfquant.channels", channels_module)
    monkeypatch.setitem(sys.modules, "cfquant.cfquant.normal_bridge", normal_module)
    import cfquant

    monkeypatch.setattr(cfquant, "cfquant", package, raising=False)
    monkeypatch.setenv("CFQUANT_BRIDGE_ID", "zs_qmt2")

    namespace = runpy.run_path(str(entry_path), run_name="cfquant_lowlat_test")
    context = FakeContext()
    deal = SimpleNamespace(m_strAccountID="28100046850")
    namespace["init"](context)
    namespace["deal_callback"](context, deal)
    namespace[namespace["_RUN_TIME_CALLBACK_NAME"]](context)
    namespace["handlebar"](context)

    assert published == [("trader:on_stock_trade", deal)]
    assert calls[0] == ("starter", "trade.request")
    assert calls.index("set_context") < calls.index("start")
    assert calls.index("start") < calls.index("bind_callback_account")
    assert context.run_time_calls == [
        (
            namespace["_RUN_TIME_CALLBACK_NAME"],
            "50nMilliSecond",
            "2019-01-01 00:00:00",
        )
    ]
    assert trade_bridge.poll_calls == [(100, 0), (100, 0)]
    assert trade_bridge.poll_calls
    assert "run_forever" not in calls
    publisher_call = next(item for item in calls if isinstance(item, tuple) and item[0] == "publisher")
    assert publisher_call[1] is trade_bridge
    assert publisher_call[2] == "callback.event"


def _install_entry_modules(monkeypatch, tx_starter=None, normal_starter=None, callback_bridge=None):
    package = types.ModuleType("cfquant.cfquant")
    package.__path__ = []
    tx_module = types.ModuleType("cfquant.cfquant.tx_trade_bridge")
    tx_module.start_tx_trade_bridge = tx_starter
    channels_module = types.ModuleType("cfquant.cfquant.channels")
    channels_module.channels_for_bridge = lambda bridge_id: {
        "normal": "normal.request",
        "trade": "trade.request",
        "callback": "callback.event",
    }
    channels_module.normalize_bridge_id = lambda bridge_id: bridge_id
    normal_module = types.ModuleType("cfquant.cfquant.normal_bridge")
    normal_module.start_normal_bridge = normal_starter
    normal_module.NormalQmtBridge = callback_bridge
    monkeypatch.setitem(sys.modules, "cfquant.cfquant", package)
    monkeypatch.setitem(sys.modules, "cfquant.cfquant.tx_trade_bridge", tx_module)
    monkeypatch.setitem(sys.modules, "cfquant.cfquant.channels", channels_module)
    monkeypatch.setitem(sys.modules, "cfquant.cfquant.normal_bridge", normal_module)


def test_qmt_entries_do_not_reload_bridge_modules(monkeypatch) -> None:
    root = Path(__file__).resolve().parents[1]
    calls = []

    def forbidden_reload(module):
        calls.append(("reload", module.__name__))
        raise AssertionError("QMT entry must not reload bridge modules")

    def start_normal_bridge(*args, **kwargs):
        calls.append(("normal", kwargs))
        return object()

    def start_tx_trade_bridge(*args, **kwargs):
        calls.append(("trade", kwargs))
        return object()

    monkeypatch.setattr(importlib, "reload", forbidden_reload)
    _install_entry_modules(
        monkeypatch,
        tx_starter=start_tx_trade_bridge,
        normal_starter=start_normal_bridge,
        callback_bridge=object,
    )

    for relative_path in ("qmt_scripts/CFQUANT.py", "qmt_scripts/CFQUANT_TRADE_LOWLAT.py"):
        entry_path = root / relative_path
        namespace = {}
        exec(compile(entry_path.read_text(encoding="ascii"), str(entry_path), "exec"), namespace)

    assert [kind for kind, _value in calls] == ["normal", "trade"]


def test_lowlat_entry_rebuilds_bridge_after_stop_and_uses_current_callback_publisher(monkeypatch) -> None:
    entry_path = Path(__file__).resolve().parents[1] / "qmt_scripts" / "CFQUANT_TRADE_LOWLAT.py"
    bridges = []
    starter_calls = []
    published = []

    class StubTradeBridge:
        def __init__(self, number):
            self.number = number
            self.events = []
            self.context = None
            self.tx = object()
            self.close_count = 0
            self.poll_calls = []

        def set_context(self, context):
            self.context = context
            self.events.append("set_context")

        def start(self):
            self.events.append("start")
            return self

        def poll(self, max_messages=100, timeout=0):
            self.poll_calls.append((max_messages, timeout))
            self.events.append("poll")

        def close(self):
            self.close_count += 1
            self.events.append("close")

    class StubCallbackBridge:
        def __init__(self, source_bridge):
            self.source_bridge = source_bridge

        @classmethod
        def for_callback_publisher(cls, source_bridge, **kwargs):
            source_bridge.events.append("publisher")
            publisher = cls(source_bridge)
            published.append(("publisher", source_bridge, kwargs))
            return publisher

        def bind_callback_account(self, context):
            self.source_bridge.events.append("bind")

        def publish_callback_event(self, event_name, obj):
            published.append(("event", self.source_bridge, event_name, obj))

    def start_tx_trade_bridge(*args, **kwargs):
        bridge = StubTradeBridge(len(bridges) + 1)
        bridges.append(bridge)
        starter_calls.append(kwargs)
        return bridge

    _install_entry_modules(
        monkeypatch,
        tx_starter=start_tx_trade_bridge,
        callback_bridge=StubCallbackBridge,
    )
    monkeypatch.setenv("CFQUANT_BRIDGE_ID", "zs_qmt2")
    namespace = {}
    exec(compile(entry_path.read_text(encoding="ascii"), str(entry_path), "exec"), namespace)

    assert len(bridges) == 1
    bridge1 = bridges[0]
    context1 = FakeContext()
    namespace["init"](context1)
    namespace["deal_callback"](context1, SimpleNamespace(m_strTradeID="trade-1"))
    assert bridge1.events == ["set_context", "start", "publisher", "bind"]
    assert context1.run_time_calls == [
        (
            namespace["_RUN_TIME_CALLBACK_NAME"],
            namespace["_RUN_TIME_PERIOD"],
            namespace["_RUN_TIME_START_TIME"],
        )
    ]
    assert published[-1][0:3] == ("event", bridge1, "trader:on_stock_trade")

    timer_callback = namespace[namespace["_RUN_TIME_CALLBACK_NAME"]]
    namespace["stop"](context1)
    namespace["stop"](context1)
    assert bridge1.close_count == 1
    assert namespace["_trade_bridge"] is None
    assert namespace["_callback_bridge"] is None
    timer_callback(context1)
    namespace["handlebar"](context1)
    assert bridge1.poll_calls == []

    context2 = FakeContext()
    namespace["init"](context2)
    assert len(bridges) == 2
    bridge2 = bridges[1]
    namespace["deal_callback"](context2, SimpleNamespace(m_strTradeID="trade-2"))
    namespace[namespace["_RUN_TIME_CALLBACK_NAME"]](context2)
    namespace["handlebar"](context2)
    assert bridge2 is not bridge1
    assert bridge2.context is context2
    assert bridge2.events == ["set_context", "start", "publisher", "bind", "poll", "poll"]
    assert context2.run_time_calls == [
        (
            namespace["_RUN_TIME_CALLBACK_NAME"],
            namespace["_RUN_TIME_PERIOD"],
            namespace["_RUN_TIME_START_TIME"],
        )
    ]
    assert bridge2.poll_calls == [(100, 0), (100, 0)]
    assert [item[1] for item in published if item[0] == "event"] == [bridge1, bridge2]
    assert starter_calls[0]["bridge_id"] == starter_calls[1]["bridge_id"] == "zs_qmt2"
    assert starter_calls[0]["request_channel"] == starter_calls[1]["request_channel"] == "trade.request"
    assert starter_calls[0]["account_id"] == starter_calls[1]["account_id"] == ""
    assert "account_type" not in starter_calls[0]
    assert "account_type" not in starter_calls[1]
    assert "account_locked" not in starter_calls[0]
    assert "account_locked" not in starter_calls[1]

    namespace["stop"](context2)
    namespace["stop"](context2)
    assert bridge2.close_count == 1


def test_normal_entry_rebuilds_bridge_after_stop_without_changing_first_start(monkeypatch) -> None:
    entry_path = Path(__file__).resolve().parents[1] / "qmt_scripts" / "CFQUANT.py"
    bridges = []
    starter_calls = []

    class StubNormalBridge:
        def __init__(self, number):
            self.number = number
            self.events = []
            self.context = None
            self.close_count = 0
            self.started_by_factory = True
            self.published = []

        def set_context(self, context):
            self.context = context
            self.events.append("set_context")

        def pump(self):
            self.events.append("pump")

        def close(self):
            self.close_count += 1
            self.events.append("close")

        def publish_callback_event(self, event_name, obj):
            self.published.append((event_name, obj))

    def start_normal_bridge(*args, **kwargs):
        bridge = StubNormalBridge(len(bridges) + 1)
        bridges.append(bridge)
        starter_calls.append(kwargs)
        return bridge

    _install_entry_modules(monkeypatch, normal_starter=start_normal_bridge)
    monkeypatch.setenv("CFQUANT_BRIDGE_ID", "zs_qmt2")
    namespace = {}
    exec(compile(entry_path.read_text(encoding="ascii"), str(entry_path), "exec"), namespace)

    assert len(bridges) == 1
    bridge1 = bridges[0]
    context1 = FakeContext()
    namespace["init"](context1)
    namespace["order_callback"](context1, SimpleNamespace(m_strOrderID="order-1"))
    assert bridge1.started_by_factory is True
    assert bridge1.events == ["set_context"]
    assert bridge1.published == [("trader:on_stock_order", bridge1.published[0][1])]
    assert bridge1.published[0][1].m_strOrderID == "order-1"

    namespace["stop"](context1)
    namespace["stop"](context1)
    assert bridge1.close_count == 1
    assert namespace["_cf_bridge"] is None

    context2 = FakeContext()
    namespace["init"](context2)
    bridge2 = bridges[1]
    namespace["order_callback"](context2, SimpleNamespace(m_strOrderID="order-2"))
    namespace["handlebar"](context2)
    assert bridge2 is not bridge1
    assert bridge2.context is context2
    assert bridge2.events == ["set_context", "pump"]
    assert bridge1.published[0][1].m_strOrderID == "order-1"
    assert bridge2.published[0][1].m_strOrderID == "order-2"
    assert len(starter_calls) == 2
    assert starter_calls[0]["bridge_id"] == starter_calls[1]["bridge_id"] == "zs_qmt2"
    assert starter_calls[0]["request_channel"] == starter_calls[1]["request_channel"] == "normal.request"
    assert starter_calls[0]["callback_event_channel"] == starter_calls[1]["callback_event_channel"] == "callback.event"
    assert starter_calls[0]["account_id"] == starter_calls[1]["account_id"] == ""

    namespace["stop"](context2)
    namespace["stop"](context2)
    assert bridge2.close_count == 1


def test_qmt_entry_scripts_parse_as_python36() -> None:
    root = Path(__file__).resolve().parents[1]
    for relative_path in ("qmt_scripts/CFQUANT_TRADE_LOWLAT.py", "qmt_scripts/CFQUANT.py"):
        path = root / relative_path
        ast.parse(path.read_text(encoding="ascii"), filename=str(path), feature_version=(3, 6))


def test_trade_entries_use_distinct_cooperative_nonblocking_drivers(tmp_path: Path) -> None:
    from cfquant.payload_builder import build_payload

    root = Path(__file__).resolve().parents[1]
    credit_manifest = build_payload(
        tmp_path / "credit_payload",
        account_type="CREDIT",
        account_id="28160000447",
        bridge_id="zs_qmt2_credit",
        model_name="CFQUANT_CREDIT_TRADE",
        template_name="CFQUANT_TRADE_LOWLAT.py",
        namespace="cfquant_credit",
        source_root=root,
    )
    paths = [
        root / "qmt_scripts" / "CFQUANT_TRADE_LOWLAT.py",
        Path(credit_manifest["model_path"]),
    ]
    callback_names = []
    for path in paths:
        source = path.read_text(encoding="ascii")
        tree = ast.parse(source, filename=str(path), feature_version=(3, 6))
        functions = {
            node.name
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
        }
        run_time_name = next(
            ast.literal_eval(node.value)
            for node in tree.body
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "_RUN_TIME_CALLBACK_NAME"
                for target in node.targets
            )
        )
        callback_names.append(run_time_name)
        assert run_time_name in functions
        assert "_trade_bridge.poll(max_messages=100, timeout=0)" in source
        assert "threading" not in source
        assert "multiprocessing" not in source
        assert "run_forever" not in source
        assert "while True" not in source

    assert len(callback_names) == 2
    assert len(set(callback_names)) == 2
