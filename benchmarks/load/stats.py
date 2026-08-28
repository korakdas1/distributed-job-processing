"""Deterministic stats helpers for local load benchmarks. No I/O, no Docker, no HTTP."""

from __future__ import annotations

import math
import re
from typing import Any

_MEM_RE = re.compile(
    r"^\s*(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>B|KiB|MiB|GiB|TiB|kB|MB|GB|TB|KB)\b",
    re.IGNORECASE,
)


def percentile(values: list[float], p: float) -> float:
    """Linear interpolation between closest ranks.

    Rank = (p/100) * (n-1) on the sorted sample. Empty input is an error.
    Do not report p99 when n is small; callers should gate on sample size.
    """
    if not values:
        raise ValueError("percentile requires at least one sample")
    if not 0.0 <= p <= 100.0:
        raise ValueError(f"percentile p must be in [0, 100], got {p}")
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (p / 100.0) * (len(ordered) - 1)
    low = int(math.floor(rank))
    high = min(low + 1, len(ordered) - 1)
    frac = rank - low
    return float(ordered[low] * (1.0 - frac) + ordered[high] * frac)


def latency_summary(values: list[float], *, p99_min_n: int = 50) -> dict[str, float | None]:
    """p50/p95 always when n>=1; p99 only when n >= p99_min_n."""
    if not values:
        return {"n": 0, "p50": None, "p95": None, "p99": None}
    summary: dict[str, float | None] = {
        "n": float(len(values)),
        "p50": round(percentile(values, 50), 1),
        "p95": round(percentile(values, 95), 1),
        "p99": None,
    }
    if len(values) >= p99_min_n:
        summary["p99"] = round(percentile(values, 99), 1)
    return summary


def speedup(throughput_n: float, throughput_1: float) -> float | None:
    if throughput_1 <= 0:
        return None
    return throughput_n / throughput_1


def efficiency(speedup_value: float | None, workers: int) -> float | None:
    if speedup_value is None or workers <= 0:
        return None
    return speedup_value / workers


def parse_cpu_percent(raw: str) -> float:
    """Parse Docker stats CPUPerc such as '12.34%' or '123.45%'. Values may exceed 100."""
    text = raw.strip().rstrip("%").strip()
    if not text:
        raise ValueError(f"empty CPU percent: {raw!r}")
    return float(text)


def parse_mem_mib(raw: str) -> float:
    """Parse the used side of Docker MemUsage, e.g. '50.2MiB / 15.5GiB', into MiB."""
    used = raw.split("/", 1)[0].strip()
    match = _MEM_RE.match(used)
    if match is None:
        raise ValueError(f"unrecognized memory value: {raw!r}")
    value = float(match.group("value"))
    unit = match.group("unit").lower()
    # Docker uses IEC (MiB) and sometimes SI (MB). Convert both to MiB.
    factors_to_mib = {
        "b": 1.0 / (1024.0 * 1024.0),
        "kib": 1.0 / 1024.0,
        "kb": 1000.0 / (1024.0 * 1024.0),
        "mib": 1.0,
        "mb": 1_000_000.0 / (1024.0 * 1024.0),
        "gib": 1024.0,
        "gb": 1_000_000_000.0 / (1024.0 * 1024.0),
        "tib": 1024.0 * 1024.0,
        "tb": 1_000_000_000_000.0 / (1024.0 * 1024.0),
    }
    return value * factors_to_mib[unit]


def round_rate(value: float) -> float:
    return round(value, 1)


def resource_peaks(samples: list[dict[str, Any]], service: str) -> dict[str, float | None]:
    cpus: list[float] = []
    mems: list[float] = []
    for sample in samples:
        row = sample.get(service)
        if not isinstance(row, dict):
            continue
        cpu = row.get("cpu_percent")
        mem = row.get("mem_mib")
        if isinstance(cpu, (int, float)):
            cpus.append(float(cpu))
        if isinstance(mem, (int, float)):
            mems.append(float(mem))
    if not cpus and not mems:
        return {
            "mean_cpu_percent": None,
            "peak_cpu_percent": None,
            "mean_mem_mib": None,
            "peak_mem_mib": None,
            "samples": 0,
        }
    return {
        "mean_cpu_percent": round(sum(cpus) / len(cpus), 1) if cpus else None,
        "peak_cpu_percent": round(max(cpus), 1) if cpus else None,
        "mean_mem_mib": round(sum(mems) / len(mems), 1) if mems else None,
        "peak_mem_mib": round(max(mems), 1) if mems else None,
        "samples": len(samples),
    }
