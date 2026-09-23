from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Iterable, Optional, Union


PathLike = Union[str, Path]
_NAMESPACE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _required_text(value: object, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _replace_once(source: str, old: str, new: str) -> str:
    count = source.count(old)
    if count != 1:
        raise ValueError(f"expected exactly one template marker {old!r}, found {count}")
    return source.replace(old, new, 1)


def _source_revision(repository: Path) -> tuple[Optional[str], Optional[bool]]:
    command = ["git", "-c", f"safe.directory={repository}", "-C", str(repository)]
    try:
        commit = subprocess.run(
            command + ["rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        if commit.returncode != 0:
            return None, None
        status = subprocess.run(
            command + ["status", "--porcelain"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        dirty = None if status.returncode != 0 else bool(status.stdout.strip())
        return commit.stdout.strip() or None, dirty
    except (OSError, subprocess.SubprocessError):
        return None, None


def render_locked_trade_model(
    template_source: str,
    *,
    account_type: str,
    account_id: str,
    bridge_id: str,
    namespace: str,
) -> str:
    resolved_account_type = _required_text(account_type, "account_type").upper()
    resolved_account_id = _required_text(account_id, "account_id")
    resolved_bridge_id = _required_text(bridge_id, "bridge_id")
    resolved_namespace = _required_text(namespace, "namespace")
    if resolved_bridge_id == "default":
        raise ValueError("bridge_id must be explicit and non-default")
    if not _NAMESPACE_RE.fullmatch(resolved_namespace):
        raise ValueError("namespace must be a valid single Python package name")

    rendered = template_source
    rendered = _replace_once(
        rendered,
        'DEFAULT_ACCOUNT_ID = ""',
        f"DEFAULT_ACCOUNT_ID = {resolved_account_id!r}",
    )
    rendered = _replace_once(
        rendered,
        'ACCOUNT_TYPE = "STOCK"',
        f"ACCOUNT_TYPE = {resolved_account_type!r}",
    )
    rendered = _replace_once(
        rendered,
        "ACCOUNT_LOCKED = False",
        "ACCOUNT_LOCKED = True",
    )
    rendered = _replace_once(
        rendered,
        'USER_BRIDGE_ID = "default"',
        f"USER_BRIDGE_ID = {resolved_bridge_id!r}",
    )
    callback_name = f"_{resolved_namespace}_trade_lowlat_pump"
    rendered = _replace_once(
        rendered,
        '_RUN_TIME_CALLBACK_NAME = "_cfquant_trade_lowlat_pump"',
        f"_RUN_TIME_CALLBACK_NAME = {callback_name!r}",
    )
    rendered = _replace_once(
        rendered,
        "def _cfquant_trade_lowlat_pump(ContextInfo):",
        f"def {callback_name}(ContextInfo):",
    )
    rendered = rendered.replace(
        "import cfquant.cfquant.tx_trade_bridge as tx_trade_bridge",
        f"import {resolved_namespace}.cfquant.tx_trade_bridge as tx_trade_bridge",
    )
    rendered = rendered.replace(
        "from cfquant.cfquant.channels import channels_for_bridge, normalize_bridge_id",
        f"from {resolved_namespace}.cfquant.channels import channels_for_bridge, normalize_bridge_id",
    )
    rendered = rendered.replace(
        "from cfquant.cfquant.normal_bridge import NormalQmtBridge",
        f"from {resolved_namespace}.cfquant.normal_bridge import NormalQmtBridge",
    )
    return rendered


def _copy_python_package(source_dir: Path, destination_dir: Path) -> list[str]:
    if not source_dir.is_dir():
        raise FileNotFoundError(f"cfquant package directory not found: {source_dir}")
    destination_dir.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    for source in sorted(source_dir.glob("*.py")):
        destination = destination_dir / source.name
        shutil.copy2(source, destination)
        copied.append(source.name)
    if "__init__.py" not in copied:
        raise ValueError("cfquant package is missing __init__.py")
    return copied


def build_payload(
    output_dir: PathLike,
    *,
    account_type: str,
    account_id: str,
    bridge_id: str,
    model_name: str,
    template_name: str,
    namespace: str,
    source_root: Optional[PathLike] = None,
    deployment_python_root: Optional[str] = None,
    instance_id: Optional[str] = None,
) -> dict[str, object]:
    repository = (
        Path(source_root).expanduser().resolve()
        if source_root is not None
        else Path(__file__).resolve().parents[1]
    )
    resolved_account_type = _required_text(account_type, "account_type").upper()
    resolved_account_id = _required_text(account_id, "account_id")
    resolved_bridge_id = _required_text(bridge_id, "bridge_id")
    resolved_model_name = _required_text(model_name, "model_name")
    resolved_template_name = _required_text(template_name, "template_name")
    resolved_namespace = _required_text(namespace, "namespace")
    if resolved_bridge_id == "default":
        raise ValueError("bridge_id must be explicit and non-default")
    if Path(resolved_model_name).name != resolved_model_name:
        raise ValueError("model_name must not contain a path")
    if Path(resolved_template_name).name != resolved_template_name:
        raise ValueError("template_name must not contain a path")
    if not resolved_template_name.endswith(".py"):
        raise ValueError("template_name must be a Python source file")
    if not _NAMESPACE_RE.fullmatch(resolved_namespace):
        raise ValueError("namespace must be a valid single Python package name")

    template_path = repository / "qmt_scripts" / resolved_template_name
    package_source = repository / "cfquant"
    if not template_path.is_file():
        raise FileNotFoundError(f"CFQuant trade template not found: {template_path}")

    destination = Path(output_dir).expanduser().resolve()
    model_dir = destination / "model"
    isolated_root = destination / "python" / resolved_namespace
    package_dir = isolated_root / "cfquant"
    model_dir.mkdir(parents=True, exist_ok=True)
    isolated_root.mkdir(parents=True, exist_ok=True)
    (isolated_root / "__init__.py").write_text(
        '"""Isolated CFQuant runtime generated from one controlled source tree."""\n',
        encoding="utf-8",
    )

    template_source = template_path.read_text(encoding="gbk")
    rendered = render_locked_trade_model(
        template_source,
        account_type=resolved_account_type,
        account_id=resolved_account_id,
        bridge_id=resolved_bridge_id,
        namespace=resolved_namespace,
    )
    model_path = model_dir / f"{resolved_model_name}.py"
    model_path.write_text(rendered, encoding="gbk")
    copied_files = _copy_python_package(package_source, package_dir)
    source_commit, source_dirty = _source_revision(repository)

    deployment_target: dict[str, object] = {
        "namespace": resolved_namespace,
    }
    if deployment_python_root:
        root = str(deployment_python_root).strip().rstrip("/\\")
        deployment_target["python_root"] = f"{root}/{resolved_namespace}/"
    if instance_id:
        deployment_target["instance_id"] = str(instance_id).strip()

    manifest: dict[str, object] = {
        "schema": "cfquant-payload/v1",
        "model_name": resolved_model_name,
        "template": resolved_template_name,
        "account_id": resolved_account_id,
        "account_type": resolved_account_type,
        "account_locked": True,
        "bridge_id": resolved_bridge_id,
        "callback_source": "trade_model",
        "namespace": resolved_namespace,
        "runtime_callback": f"_{resolved_namespace}_trade_lowlat_pump",
        "source_root": str(repository),
        "source_commit": source_commit,
        "source_dirty": source_dirty,
        "model_path": str(model_path),
        "isolated_package_root": str(isolated_root),
        "isolated_import": f"{resolved_namespace}.cfquant",
        "package_files": copied_files,
        "deployment_target": deployment_target,
        "notes": [
            "This payload does not edit QMT GUI model indexes.",
            "The model and isolated package are generated from one CFQuant source tree.",
            "No generated bytecode is copied into the payload.",
        ],
    }
    manifest_path = destination / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a locked isolated CFQuant payload")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--account-type", required=True)
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--bridge-id", required=True)
    parser.add_argument("--model-name", required=True)
    parser.add_argument("--template-name", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--source-root", default=None)
    parser.add_argument("--deployment-python-root", default=None)
    parser.add_argument("--instance-id", default=None)
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    manifest = build_payload(
        args.output_dir,
        account_type=args.account_type,
        account_id=args.account_id,
        bridge_id=args.bridge_id,
        model_name=args.model_name,
        template_name=args.template_name,
        namespace=args.namespace,
        source_root=args.source_root,
        deployment_python_root=args.deployment_python_root,
        instance_id=args.instance_id,
    )
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
