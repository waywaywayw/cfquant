from __future__ import annotations

import ast
import sys
from pathlib import Path
from types import SimpleNamespace


SOURCE_PATH = Path(__file__).resolve().parents[1] / "LTtx" / "tx" / "LTtx_server.py"
PIP_INDEX = "https://pypi.tuna.tsinghua.edu.cn/simple"


def _load_dependency_namespace() -> dict[str, object]:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(SOURCE_PATH))
    preamble = []
    found_invocation = False

    for node in tree.body:
        if (
            isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "ensure_modules_with_version"
        ):
            found_invocation = True
            break
        preamble.append(node)

    assert found_invocation, "dependency checker invocation moved unexpectedly"
    namespace: dict[str, object] = {"__name__": "lttx_dependency_test"}
    preamble_tree = ast.Module(body=preamble, type_ignores=[])
    exec(compile(preamble_tree, str(SOURCE_PATH), "exec"), namespace)
    return namespace


def _run_checker(monkeypatch, modules, import_module, check_call):
    namespace = _load_dependency_namespace()
    monkeypatch.setitem(namespace, "importlib", SimpleNamespace(import_module=import_module))
    monkeypatch.setitem(namespace, "subprocess", SimpleNamespace(check_call=check_call))
    namespace["ensure_modules_with_version"](modules)


def _expected_command(target):
    return [sys.executable, "-m", "pip", "install", target, "-i", PIP_INDEX]


def test_hashlib_is_not_in_startup_dependency_manifest():
    namespace = _load_dependency_namespace()

    assert "hashlib" not in namespace["need_packge"]


def test_existing_modules_without_version_are_not_installed(monkeypatch):
    imported = []
    install_calls = []

    def import_module(name):
        imported.append(name)
        return object()

    _run_checker(
        monkeypatch,
        {
            "hashlib": {"pip_name": "hashlib", "version": "0.0.1"},
            "module_without_version": {"pip_name": "versionless-pip", "version": "1.2.3"},
        },
        import_module,
        install_calls.append,
    )

    assert imported == ["hashlib", "module_without_version"]
    assert install_calls == []


def test_installed_latest_module_is_not_upgraded(monkeypatch):
    install_calls = []

    _run_checker(
        monkeypatch,
        {"calendar_module": {"pip_name": "pandas-market-calendars", "version": "latest"}},
        lambda name: SimpleNamespace(__version__="1.0.0"),
        install_calls.append,
    )

    assert install_calls == []


def test_missing_numeric_package_uses_pip_name_and_minimum_version(monkeypatch):
    install_calls = []

    def import_module(name):
        raise ImportError("module is missing")

    _run_checker(
        monkeypatch,
        {"missing_module": {"pip_name": "real-package", "version": "2.4.1"}},
        import_module,
        install_calls.append,
    )

    assert install_calls == [_expected_command("real-package>=2.4.1")]


def test_low_version_package_uses_pip_name_and_minimum_version(monkeypatch):
    install_calls = []

    _run_checker(
        monkeypatch,
        {"old_module": {"pip_name": "real-package", "version": "2.4.1"}},
        lambda name: SimpleNamespace(__version__="1.9.9"),
        install_calls.append,
    )

    assert install_calls == [_expected_command("real-package>=2.4.1")]


def test_missing_latest_package_uses_pip_name_without_typo(monkeypatch):
    install_calls = []

    def import_module(name):
        raise ImportError("module is missing")

    _run_checker(
        monkeypatch,
        {"missing_module": {"pip_name": "real-package", "version": "latest"}},
        import_module,
        install_calls.append,
    )

    assert install_calls == [_expected_command("real-package")]


def test_install_failure_is_non_fatal(monkeypatch):
    install_calls = []

    def import_module(name):
        raise ImportError("module is missing")

    def failed_check_call(command):
        install_calls.append(command)
        raise RuntimeError("offline")

    _run_checker(
        monkeypatch,
        {"missing_module": {"pip_name": "real-package", "version": "2.4.1"}},
        import_module,
        failed_check_call,
    )

    assert install_calls == [_expected_command("real-package>=2.4.1")]
