from __future__ import annotations

import sys
import threading
from pathlib import Path

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
    def __init__(self) -> None:
        self.quote_callback = None

    def get_full_tick(self, code_list: list[str]) -> dict[str, dict[str, float]]:
        return {code: {"lastPrice": 12.34} for code in code_list}

    def subscribe_quote(
        self,
        stock_code: str,
        period: str,
        start_time: str = "",
        end_time: str = "",
        count: int = 0,
        callback=None,
    ) -> int:
        self.quote_callback = callback
        return 17

    def unsubscribe_quote(self, subscribe_id: int) -> bool:
        return subscribe_id == 17


def test_queued_request_waits_for_qmt_callback() -> None:
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
    assert bridge.worker_thread is None


def test_queued_request_is_processed_on_qmt_timer() -> None:
    context = FakeContext()
    bridge = NormalQmtBridge(context=context, globals_dict={})
    bridge.running = True
    bridge.tx = FakeTx()

    try:
        bridge._handle_raw_from_thread(
            pack_request(
                "xtdata.get_full_tick",
                {"code_list": ["603458.SH"]},
                client_id="client-1",
                request_id="request-1",
            )
        )
        bridge._on_timer()

        assert bridge.tx.response_ready.wait(1)
        kind, payload, channel = bridge.tx.pushed[-1]
        response = loads_message(payload)
        assert kind == "response"
        assert channel == "client-1"
        assert response["ok"] is True
        assert response["result"] == {"603458.SH": {"lastPrice": 12.34}}
    finally:
        bridge.close()


def test_repeated_timer_callbacks_drain_request_backlog() -> None:
    context = FakeContext()
    bridge = NormalQmtBridge(
        context=context,
        globals_dict={},
        pump_max_count=1,
    )
    bridge.running = True
    bridge.tx = FakeTx()

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

        for _ in range(3):
            bridge._on_timer()

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


def test_quote_subscription_uses_native_qmt_callback() -> None:
    context = FakeContext()
    bridge = NormalQmtBridge(context=context, globals_dict={})
    bridge.running = True
    bridge.tx = FakeTx()

    try:
        bridge._handle_raw_from_thread(
            pack_request(
                "xtdata.subscribe_quote",
                {
                    "stock_code": "603458.SH",
                    "period": "tick",
                    "start_time": "",
                    "end_time": "",
                    "count": 0,
                },
                client_id="client-1",
                request_id="subscribe-1",
            )
        )

        assert bridge.tx.pushed == []
        bridge._on_timer()

        kind, payload, channel = bridge.tx.pushed[-1]
        response = loads_message(payload)
        assert kind == "response"
        assert channel == "client-1"
        assert response["result"] == {"subscribe_id": 17}
        assert 17 in bridge.subscriptions

        context.quote_callback({"603458.SH": {"lastPrice": 12.35}})

        kind, payload, channel = bridge.tx.pushed[-1]
        event = loads_message(payload)
        assert kind == "event"
        assert channel == "client-1"
        assert event["event"] == "quote:17"
        assert event["data"] == {"603458.SH": {"lastPrice": 12.35}}
    finally:
        bridge.close()
