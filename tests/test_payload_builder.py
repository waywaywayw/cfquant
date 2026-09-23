from __future__ import annotations

import json
from pathlib import Path

import pytest

from cfquant.payload_builder import build_payload, render_locked_trade_model


def _fake_source(tmp_path: Path) -> Path:
    root = tmp_path / "source"
    (root / "qmt_scripts").mkdir(parents=True)
    (root / "cfquant").mkdir()
    (root / "cfquant" / "__init__.py").write_text("", encoding="utf-8")
    (root / "cfquant" / "tx_trade_bridge.py").write_text("VALUE = 1\n", encoding="utf-8")
    (root / "cfquant" / "normal_bridge.py").write_text("VALUE = 2\n", encoding="utf-8")
    (root / "cfquant" / "channels.py").write_text("VALUE = 3\n", encoding="utf-8")
    (root / "cfquant" / "ignored.pyc").write_bytes(b"cache")
    (root / "qmt_scripts" / "CFQUANT_TRADE_LOWLAT.py").write_text(
        "\n".join(
            [
                'DEFAULT_ACCOUNT_ID = ""',
                'ACCOUNT_TYPE = "STOCK"',
                "ACCOUNT_LOCKED = False",
                'USER_BRIDGE_ID = "default"',
                '_RUN_TIME_CALLBACK_NAME = "_cfquant_trade_lowlat_pump"',
                "def _cfquant_trade_lowlat_pump(ContextInfo):",
                "    pass",
                "import cfquant.cfquant.tx_trade_bridge as tx_trade_bridge",
                "from cfquant.cfquant.channels import channels_for_bridge, normalize_bridge_id",
                "from cfquant.cfquant.normal_bridge import NormalQmtBridge",
                "",
            ]
        ),
        encoding="gbk",
    )
    return root


@pytest.mark.parametrize("account_type", ["STOCK", "CREDIT", "FUTURE"])
def test_build_payload_is_parameterized_and_excludes_generated_cache(
    tmp_path: Path,
    account_type: str,
) -> None:
    source = _fake_source(tmp_path)
    manifest = build_payload(
        tmp_path / "out",
        account_type=account_type,
        account_id="account-1",
        bridge_id="bridge-1",
        model_name="LOCKED_MODEL",
        template_name="CFQUANT_TRADE_LOWLAT.py",
        namespace="cfquant_isolated",
        source_root=source,
        deployment_python_root="C:/QMT/python",
        instance_id="qmt-instance-1",
    )

    model = Path(manifest["model_path"]).read_text(encoding="gbk")
    assert "DEFAULT_ACCOUNT_ID = 'account-1'" in model
    assert f"ACCOUNT_TYPE = '{account_type}'" in model
    assert "ACCOUNT_LOCKED = True" in model
    assert "USER_BRIDGE_ID = 'bridge-1'" in model
    assert "_RUN_TIME_CALLBACK_NAME = '_cfquant_isolated_trade_lowlat_pump'" in model
    assert "def _cfquant_isolated_trade_lowlat_pump(ContextInfo):" in model
    assert "cfquant_isolated.cfquant.tx_trade_bridge" in model
    assert "cfquant.cfquant.tx_trade_bridge" not in model
    assert "_RUN_TIME_CALLBACK_NAME = '_cfquant_isolated_trade_lowlat_pump'" in model
    assert "def _cfquant_isolated_trade_lowlat_pump(ContextInfo):" in model

    isolated = Path(manifest["isolated_package_root"])
    assert (isolated / "cfquant" / "tx_trade_bridge.py").is_file()
    assert not (isolated / "cfquant" / "ignored.pyc").exists()

    on_disk = json.loads(Path(manifest["manifest_path"]).read_text(encoding="utf-8"))
    assert on_disk["schema"] == "cfquant-payload/v1"
    assert on_disk["template"] == "CFQUANT_TRADE_LOWLAT.py"
    assert on_disk["namespace"] == "cfquant_isolated"
    assert on_disk["runtime_callback"] == "_cfquant_isolated_trade_lowlat_pump"
    assert on_disk["source_commit"] is None
    assert on_disk["deployment_target"] == {
        "instance_id": "qmt-instance-1",
        "namespace": "cfquant_isolated",
        "python_root": "C:/QMT/python/cfquant_isolated/",
    }


def test_builder_rejects_missing_account_and_default_bridge(tmp_path: Path) -> None:
    source = _fake_source(tmp_path)
    kwargs = dict(
        output_dir=tmp_path / "out",
        account_type="FUTURE",
        model_name="LOCKED_MODEL",
        template_name="CFQUANT_TRADE_LOWLAT.py",
        namespace="cfquant_isolated",
        source_root=source,
    )
    with pytest.raises(ValueError, match="account_id is required"):
        build_payload(account_id="", bridge_id="bridge-1", **kwargs)
    with pytest.raises(ValueError, match="non-default"):
        build_payload(account_id="account-1", bridge_id="default", **kwargs)


def test_render_rejects_invalid_namespace() -> None:
    source = "\n".join(
        [
            'DEFAULT_ACCOUNT_ID = ""',
            'ACCOUNT_TYPE = "STOCK"',
            "ACCOUNT_LOCKED = False",
            'USER_BRIDGE_ID = "default"',
            '_RUN_TIME_CALLBACK_NAME = "_cfquant_trade_lowlat_pump"',
            "def _cfquant_trade_lowlat_pump(ContextInfo):",
            "    pass",
        ]
    )
    with pytest.raises(ValueError, match="namespace"):
        render_locked_trade_model(
            source,
            account_type="STOCK",
            account_id="account-1",
            bridge_id="bridge-1",
            namespace="not-valid-name",
        )
