from __future__ import annotations

import threading
import time

import cfquant_web_server as web


def test_status_monitor_wake_runs_once_without_entering_tight_loop(monkeypatch) -> None:
    calls = 0
    second_probe = threading.Event()

    def probe_bridge_status(*, bridge_id, timeout):  # noqa: ANN001
        nonlocal calls
        calls += 1
        if calls >= 2:
            second_probe.set()
        return {"normal": {"online": True}, "trade": {"online": True}}

    monkeypatch.setattr(web, "current_bridges", lambda: ["default"])
    monkeypatch.setattr(web, "probe_bridge_status", probe_bridge_status)
    monkeypatch.setattr(web, "bridge_config", lambda bridge_id: {"name": bridge_id})

    monitor = web.ChannelStatusMonitor(interval=60.0, timeout=0.1)
    try:
        monitor.start()
        deadline = time.time() + 1.0
        while calls < 1 and time.time() < deadline:
            time.sleep(0.01)
        assert calls == 1

        monitor.wake()
        assert second_probe.wait(1.0)
        time.sleep(0.05)
        assert calls == 2
    finally:
        monitor.close()
