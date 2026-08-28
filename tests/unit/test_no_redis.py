from __future__ import annotations

from pathlib import Path

SRC = Path("src/job_platform")
API = SRC / "api"


def test_api_does_not_execute_handlers() -> None:
    banned = ("execute_handler", "BackgroundTasks", "create_task(")
    for path in API.rglob("*.py"):
        text = path.read_text()
        for token in banned:
            assert token not in text, f"{token} found in {path}"


def test_api_does_not_publish_to_redis() -> None:
    banned = ("publish_job_id", "xadd", "XADD")
    for path in API.rglob("*.py"):
        text = path.read_text()
        for token in banned:
            assert token not in text, f"{token} found in {path}"


def test_no_high_level_job_frameworks_or_eval() -> None:
    banned = (
        "import celery",
        "from celery",
        "import dramatiq",
        "from dramatiq",
        "from rq ",
        "import rq",
        "eval(",
        "exec(",
    )
    xauto_allowed = {SRC / "queue" / "streams.py"}
    for path in SRC.rglob("*.py"):
        text = path.read_text()
        for token in banned:
            assert token not in text, f"{token} found in {path}"
        if path.resolve() not in {item.resolve() for item in xauto_allowed}:
            assert "XAUTOCLAIM" not in text, f"XAUTOCLAIM found in {path}"
            assert "xautoclaim(" not in text, f"xautoclaim found in {path}"
