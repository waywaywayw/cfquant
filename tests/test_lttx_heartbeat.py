from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest


SOURCE = Path(__file__).resolve().parents[1] / "LTtx/tx/LTtx_server.py"


def load_function(name, namespace):
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace[name]


def test_heartbeat_survives_channel_removal_during_delivery():
    channels = {}
    delivered = []

    def deliver(message):
        delivered.append(message)
        channels.clear()

    class EndCycle(Exception):
        pass

    def sleep(seconds):
        raise EndCycle

    channels["a"] = {"que": SimpleNamespace(put=deliver)}
    heartbeat = load_function("main_heartbeat", {
        "dict_client_push_group": channels,
        "time": SimpleNamespace(sleep=sleep),
    })
    with pytest.raises(EndCycle):
        heartbeat()
    assert delivered == [(1, {1: 1})]


def test_peer_eof_stops_heartbeat_reader_without_spinning():
    calls = []

    def recv(*args):
        calls.append(args)
        if len(calls) > 1:
            raise ConnectionError("reader must already have exited")
        return b""

    heartbeat = load_function("main_txg_heartbeat", {"socket": SimpleNamespace(MSG_WAITALL=256)})
    heartbeat(SimpleNamespace(recv=recv))
    assert len(calls) == 1
