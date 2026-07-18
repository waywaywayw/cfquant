from __future__ import annotations

import sys
import threading
import time
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
    def get_full_tick(self, code_list: list[str]) -> dict[str, dict[str, float]]:
        return {code: {"lastPrice": 12.34} for code in code_list}


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
