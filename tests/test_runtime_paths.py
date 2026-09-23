from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from cfquant.qmt_bridge import CfquantQmtBridge
from cfquant.tx_trade_bridge import TxTradeBridge


ROOT = Path(__file__).resolve().parents[1]


def test_bridge_logs_support_explicit_external_path(monkeypatch, tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    monkeypatch.setenv("CFQUANT_LOG_DIR", str(log_dir))
    monkeypatch.delenv("CFQUANT_BRIDGE_LOG_FILE", raising=False)

    trade = TxTradeBridge(context=None, show=False)
    normal = CfquantQmtBridge(context=None, show=False)

    expected = log_dir / "cfquant_qmt_bridge.log"
    assert Path(trade.log_file) == expected
    assert Path(normal.log_file) == expected


def test_web_runtime_paths_can_be_fully_externalized(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    log_dir = tmp_path / "logs"
    tmp_dir = tmp_path / "tmp"
    env = os.environ.copy()
    env.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": str(ROOT),
            "CFQUANT_STATE_DIR": str(state_dir),
            "CFQUANT_LOG_DIR": str(log_dir),
            "CFQUANT_TMP_DIR": str(tmp_dir),
            "CFQUANT_LTTX_LOG_DIR": str(log_dir / "lttx"),
            "CFQUANT_TX_LOG_DIR": str(log_dir / "tx"),
        }
    )
    command = [
        sys.executable,
        "-c",
        (
            "import json, cfquant_web_server as web; "
            "print(json.dumps({"
            "'state': web.STATE_DIR, 'log': web.LOG_DIR, 'tmp': web.TMP_DIR, "
            "'config': web.WEB_CONFIG_FILE, 'db': web.WEB_SETTINGS_DB_FILE, "
            "'web_log': web.LOG_FILE, 'lttx_out': web.LTTX_STDOUT_LOG, "
            "'lttx_err': web.LTTX_STDERR_LOG, 'lttx_log': web.LTTX_LOG_DIR, "
            "'lttx_state': web.LTTX_STATE_DIR, 'lttx_file': web.LTTX_FILE_DIR, "
            "'lttx_dataframe': web.LTTX_DATAFRAME_DIR, 'tx_log': web.TX_LOG_DIR"
            "}))"
        ),
    ]
    completed = subprocess.run(
        command,
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout.strip().splitlines()[-1])

    assert payload["state"] == str(state_dir)
    assert payload["log"] == str(log_dir)
    assert payload["tmp"] == str(tmp_dir)
    assert payload["config"] == str(state_dir / "cfquant_web_config.json")
    assert payload["db"] == str(state_dir / "cfquant_web_config.db")
    assert payload["web_log"] == str(log_dir / "cfquant_web_server.runtime.log")
    assert payload["lttx_out"] == str(log_dir / "lttx_server.stdout.log")
    assert payload["lttx_err"] == str(log_dir / "lttx_server.stderr.log")
    assert payload["lttx_log"] == str(log_dir / "lttx")
    assert payload["lttx_state"] == str(state_dir / "lttx")
    assert payload["lttx_file"] == str(tmp_dir / "lttx_files")
    assert payload["lttx_dataframe"] == str(tmp_dir / "lttx_dataframes")
    assert payload["tx_log"] == str(log_dir / "tx")


def test_managed_update_policy_allows_only_selected_fork_release(monkeypatch) -> None:
    import cfquant_web_server as web

    monkeypatch.setattr(web, "UPDATE_POLICY", "managed")
    monkeypatch.setattr(web, "UPDATE_REPO_URL", "https://github.com/waywaywayw/cfquant")
    monkeypatch.setattr(web, "UPDATE_REF", "release-20260922")
    updater = web.CfquantUpdater(config=None)

    updater._assert_update_allowed(
        "github",
        repo_url="https://github.com/waywaywayw/cfquant.git",
        ref="release-20260922",
    )
    with pytest.raises(RuntimeError, match="selected fork release"):
        updater._assert_update_allowed(
            "github",
            repo_url="https://github.com/95ge/cfquant",
            ref="release-20260922",
        )
    with pytest.raises(RuntimeError, match="selected fork release"):
        updater._assert_update_allowed(
            "github",
            repo_url="https://github.com/waywaywayw/cfquant",
            ref="main",
        )
    with pytest.raises(RuntimeError, match="rejects direct zip"):
        updater._assert_update_allowed("zip")


def test_lttx_and_embedded_tx_sources_use_external_runtime_environment() -> None:
    lttx_server = (ROOT / "LTtx" / "tx" / "LTtx_server.py").read_text(encoding="utf-8")
    lttx_client = (ROOT / "LTtx" / "tx" / "tx.py").read_text(encoding="utf-8")
    embedded_client = (ROOT / "qmt_scripts" / "tx.py").read_text(encoding="utf-8")

    assert 'os.environ.get("CFQUANT_LTTX_LOG_DIR")' in lttx_server
    assert 'os.environ.get("CFQUANT_LTTX_STATE_DIR")' in lttx_server
    assert 'os.environ.get("CFQUANT_LTTX_FILE_DIR")' in lttx_server
    assert 'os.environ.get("CFQUANT_LTTX_DATAFRAME_DIR")' in lttx_server
    assert "LTTX_STATE_FILE" in lttx_server
    assert "LTTX_FILE_DATA_DIR" in lttx_server
    assert "LTTX_DATAFRAME_DIR" in lttx_server
    for source in (lttx_client, embedded_client):
        assert 'os.environ.get("CFQUANT_TX_LOG_DIR")' in source
        assert "os.path.join(self.log_dir" in source
