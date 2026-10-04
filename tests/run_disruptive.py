"""Run selected integration tests against guarded disposable services."""

from __future__ import annotations

import os
import subprocess
import sys

from tests.disposable_infrastructure import ROOT, provision


def run_tests(args: list[str], *, disruptive: bool = True) -> int:
    with provision() as infrastructure:
        env = {**os.environ, **infrastructure.environment()}
        env["PYTHONPATH"] = os.pathsep.join((str(ROOT / "src"), str(ROOT)))
        env["PYTHONDONTWRITEBYTECODE"] = "1"
        command = [
            sys.executable,
            "-m",
            "pytest",
            *args,
            "--disposable-manifest",
            str(infrastructure.directory / "manifest.json"),
        ]
        if disruptive:
            command.append("--run-disruptive")
        result = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        output = result.stdout.replace(infrastructure.password, "[redacted]")
        (infrastructure.directory / "pytest.log").write_text(output)
        print(output, end="", flush=True)
        (infrastructure.directory / "result.txt").write_text(
            f"pytest exit code: {result.returncode}\n"
        )
        return result.returncode


if __name__ == "__main__":
    raise SystemExit(run_tests(sys.argv[1:] or ["tests/integration", "-m", "disruptive"]))
