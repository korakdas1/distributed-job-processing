"""Exercise pytest's actual collection/setup hooks without database or Docker access."""

from __future__ import annotations

import ast
import os

import pytest

from tests.disposable_infrastructure import ROOT

pytest_plugins = ["pytester"]


def test_all_service_control_tests_declare_disruption() -> None:
    callers: list[str] = []
    for path in (ROOT / "tests/integration").glob("test_*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            calls = [
                call
                for call in ast.walk(node)
                if isinstance(call, ast.Call)
                and isinstance(call.func, ast.Name)
                and call.func.id
                in {
                    "stop_project_postgres",
                    "start_project_postgres",
                    "stop_project_redis",
                    "start_project_redis",
                }
            ]
            if not calls:
                continue
            callers.append(node.name)
            assert "pytest.mark.disruptive" in [ast.unparse(d) for d in node.decorator_list]
            assert "disposable_infrastructure" in [arg.arg for arg in node.args.args]
            assert all(len(call.args) == 1 for call in calls)
    assert len(callers) == 7


@pytest.mark.parametrize("opt_in", [False, True])
def test_disruptive_execution_gate(
    pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch, opt_in: bool
) -> None:
    monkeypatch.setenv("PYTHONPATH", os.pathsep.join((str(ROOT), str(ROOT / "src"))))
    pytester.makeconftest((ROOT / "tests/conftest.py").read_text())
    pytester.makeini("[pytest]\nmarkers = disruptive: service outage\n")
    pytester.makepyfile("""
import pytest
@pytest.fixture(autouse=True)
def database():
    raise AssertionError("Database setup must not run")
@pytest.mark.disruptive
def test_outage():
    raise AssertionError("Service control must not run")
""")
    result = pytester.runpytest_subprocess(*(["--run-disruptive"] if opt_in else []))
    if opt_in:
        result.assert_outcomes(errors=1)
        assert "Use python -m tests.run_disruptive" in result.stdout.str()
        assert "Database setup must not run" not in result.stdout.str()
    else:
        result.assert_outcomes(skipped=1)
