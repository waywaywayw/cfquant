from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cfquant.normal_bridge import NormalQmtBridge
from cfquant.protocol import loads_message, pack_request


class FakeTx:
    def __init__(self) -> None:
        self.pushed: list[tuple[str, str, str]] = []
        self.response_ready = threading.Event()

    def push(self, kind: str, payload: str, channel: str) -> None:
        self.pushed.append((kind, payload, channel))
        self.response_ready.set()

    def close(self) -> None:
        return None


class FakeContext:
    def __init__(self, set_account_error=None, events=None) -> None:
        self.whole_quote_calls: list[tuple[list[str], object]] = []
        self.quote_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.unsubscribed: list[int] = []
        self.account_calls: list[str] = []
        self.set_account_error = set_account_error
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
