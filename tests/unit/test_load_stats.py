"""Unit tests for load-benchmark helpers. These must not start Docker or submit jobs."""

from benchmarks.load.stats import (
    efficiency,
    latency_summary,
    parse_cpu_percent,
    parse_mem_mib,
    percentile,
    resource_peaks,
    speedup,
)
from benchmarks.load.workloads import FULL, QUICK, WORD_COUNT_TEXT, word_count_body


def test_percentile_linear_interpolation() -> None:
    samples = [10.0, 20.0, 30.0, 40.0, 50.0]
    assert percentile(samples, 0) == 10.0
    assert percentile(samples, 100) == 50.0
    assert percentile(samples, 50) == 30.0
    assert abs(percentile(samples, 90) - 46.0) < 1e-9


def test_latency_summary_omits_p99_for_tiny_n() -> None:
    summary = latency_summary([1.0, 2.0, 3.0])
    assert summary["n"] == 3
    assert summary["p50"] == 2.0
    assert summary["p99"] is None


def test_latency_summary_includes_p99_when_n_large() -> None:
    values = [float(i) for i in range(50)]
    summary = latency_summary(values)
    assert summary["p99"] is not None
    assert summary["n"] == 50


def test_speedup_and_efficiency() -> None:
    assert speedup(170.0, 100.0) == 1.7
    assert efficiency(1.7, 2) == 0.85
    assert speedup(10.0, 0.0) is None
    assert efficiency(None, 4) is None


def test_parse_cpu_may_exceed_100() -> None:
    assert parse_cpu_percent("12.34%") == 12.34
    assert parse_cpu_percent(" 250.00% ") == 250.0


def test_parse_mem_mib() -> None:
    assert abs(parse_mem_mib("50.0MiB / 15.5GiB") - 50.0) < 1e-9
    assert abs(parse_mem_mib("1.0GiB / 16GiB") - 1024.0) < 1e-9
    assert parse_mem_mib("1024KiB / 1GiB") == 1.0


def test_resource_peaks_aggregates_service() -> None:
    samples = [
        {"postgres": {"cpu_percent": 10.0, "mem_mib": 80.0}},
        {"postgres": {"cpu_percent": 30.0, "mem_mib": 100.0}},
    ]
    summary = resource_peaks(samples, "postgres")
    assert summary["mean_cpu_percent"] == 20.0
    assert summary["peak_cpu_percent"] == 30.0
    assert summary["peak_mem_mib"] == 100.0


def test_workloads_are_fixed_and_quick_is_smaller() -> None:
    assert "the quick brown fox" in WORD_COUNT_TEXT
    assert word_count_body()["payload"]["text"] == WORD_COUNT_TEXT
    assert QUICK.repeats == 1
    assert FULL.repeats == 3
    assert QUICK.lightweight_jobs < FULL.lightweight_jobs
    assert QUICK.workers == (1, 2)
    assert FULL.workers == (1, 2, 4)
