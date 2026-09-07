from __future__ import annotations

import queue
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cfquant.client import CfquantTimeout, LTtxRpcClient


class BlockingTransport:
    def __init__(self, *args: object) -> None:
        self.Q: queue.Queue[object] = queue.Queue()
        self.closed = False

    def start_tx(self) -> None:
        while not self.closed:
            time.sleep(0.005)

    def start_txg(self, client_id: str) -> None:
        raise AssertionError("start_txg should not run after timeout")

    def close(self) -> None:
        self.closed = True


def test_connect_timeout_is_end_to_end(monkeypatch) -> None:
    client = LTtxRpcClient(timeout=0.02)
    transports: list[BlockingTransport] = []

    def factory(*args: object) -> BlockingTransport:
        transport = BlockingTransport(*args)
        transports.append(transport)
        return transport

    monkeypatch.setattr(client, "_load_txl", lambda: factory)
    started = time.monotonic()

    with pytest.raises(CfquantTimeout, match="connect timeout"):
        client.request("cfquant.status")

    assert time.monotonic() - started < 0.5
    assert len(transports) == 1
    assert transports[0].closed is True


def test_request_timeout_closes_transport(monkeypatch) -> None:
    client = LTtxRpcClient(timeout=0.01)
    closed: list[bool] = []
    monkeypatch.setattr(client, "start", lambda: None)
    monkeypatch.setattr(client, "_push", lambda *args, **kwargs: None)
    monkeypatch.setattr(client, "close", lambda: closed.append(True))

    with pytest.raises(CfquantTimeout, match="request timeout"):
        client.request("cfquant.status", timeout=0.01)

    assert closed == [True]
