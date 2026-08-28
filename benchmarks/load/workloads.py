"""Fixed payloads and profile sizes. Importing does not run benchmarks."""

from __future__ import annotations

from dataclasses import dataclass, field

WORD_COUNT_TEXT = (
    "the quick brown fox jumps over the lazy dog " * 8
).strip()  # fixed representative length
SLEEP_SECONDS = 0.5
PRIME_LIMIT = 100_000  # handler safety cap; calibration records actual duration
WARMUP_JOBS_FULL = 20
WARMUP_JOBS_QUICK = 5

PRIORITY_MIX_FULL = {"CRITICAL": 48, "HIGH": 20, "NORMAL": 16, "LOW": 16}
PRIORITY_MIX_QUICK = {"CRITICAL": 8, "HIGH": 4, "NORMAL": 4, "LOW": 4}


@dataclass(frozen=True)
class ProfileConfig:
    name: str
    repeats: int
    workers: tuple[int, ...]
    submission_jobs: int
    submission_conc: tuple[int, ...]
    lightweight_jobs: int
    lightweight_conc: int
    sleep_jobs: int
    cpu_jobs: int
    warmup_jobs: int
    priority_mix: dict[str, int] = field(default_factory=dict)
    poll_seconds: float = 0.4
    run_timeout_seconds: float = 180.0
    sleep_timeout_seconds: float = 180.0


FULL = ProfileConfig(
    name="full",
    repeats=3,
    workers=(1, 2, 4),
    submission_jobs=150,
    submission_conc=(1, 10, 25, 50),
    lightweight_jobs=200,
    lightweight_conc=25,
    sleep_jobs=40,
    cpu_jobs=50,
    warmup_jobs=WARMUP_JOBS_FULL,
    priority_mix=dict(PRIORITY_MIX_FULL),
)

QUICK = ProfileConfig(
    name="quick",
    repeats=1,
    workers=(1, 2),
    submission_jobs=20,
    submission_conc=(1, 10),
    lightweight_jobs=20,
    lightweight_conc=10,
    sleep_jobs=8,
    cpu_jobs=8,
    warmup_jobs=WARMUP_JOBS_QUICK,
    priority_mix=dict(PRIORITY_MIX_QUICK),
    run_timeout_seconds=90.0,
    sleep_timeout_seconds=90.0,
)


def word_count_body(priority: str = "NORMAL") -> dict[str, object]:
    return {
        "job_type": "word_count",
        "payload": {"text": WORD_COUNT_TEXT},
        "priority": priority,
    }


def sleep_body(priority: str = "NORMAL") -> dict[str, object]:
    return {
        "job_type": "sleep",
        "payload": {"seconds": SLEEP_SECONDS},
        "priority": priority,
        "timeout_seconds": 30,
    }


def prime_body(limit: int = PRIME_LIMIT, priority: str = "NORMAL") -> dict[str, object]:
    return {
        "job_type": "prime_calculation",
        "payload": {"limit": limit},
        "priority": priority,
        "timeout_seconds": 30,
    }
