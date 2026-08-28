from job_platform.core.enums import JobPriority
from job_platform.queue.priority import (
    WeightedPrioritySelector,
    stream_for_priority,
)


def test_stream_for_priority_mapping() -> None:
    assert stream_for_priority(JobPriority.CRITICAL) == "jobs:critical"
    assert stream_for_priority("HIGH") == "jobs:high"
    assert stream_for_priority(JobPriority.NORMAL) == "jobs:normal"
    assert stream_for_priority(JobPriority.LOW) == "jobs:low"


def test_stream_for_priority_unknown_raises() -> None:
    try:
        stream_for_priority("URGENT")
    except Exception as exc:
        assert "Unknown job priority" in str(exc)
    else:
        raise AssertionError("expected UnknownPriorityError")


def _selector() -> WeightedPrioritySelector:
    return WeightedPrioritySelector(
        ("jobs:critical", "jobs:high", "jobs:normal", "jobs:low"),
        {
            "jobs:critical": 8,
            "jobs:high": 4,
            "jobs:normal": 2,
            "jobs:low": 1,
        },
    )


def test_weighted_quota_cycle_when_all_streams_available() -> None:
    selector = _selector()
    available = {"jobs:critical", "jobs:high", "jobs:normal", "jobs:low"}
    counts = {name: 0 for name in available}
    for _ in range(15):
        chosen = selector.select_available(available)
        assert chosen is not None
        counts[chosen] += 1
    assert counts == {
        "jobs:critical": 8,
        "jobs:high": 4,
        "jobs:normal": 2,
        "jobs:low": 1,
    }


def test_empty_critical_does_not_block_lower_streams() -> None:
    selector = _selector()
    available = {"jobs:high", "jobs:normal", "jobs:low"}
    chosen = [selector.select_available(available) for _ in range(7)]
    assert chosen[0] == "jobs:high"
    assert "jobs:critical" not in chosen
    assert set(chosen) <= available


def test_low_is_selected_while_critical_is_backlogged() -> None:
    selector = _selector()
    available = {"jobs:critical", "jobs:low"}
    chosen: list[str] = []
    for _ in range(15):
        stream = selector.select_available(available)
        assert stream is not None
        chosen.append(stream)
    assert "jobs:low" in chosen
    assert "jobs:critical" in chosen
    assert set(chosen) <= available
