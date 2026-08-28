"""Domain enumerations shared by models, schemas, and the API."""

from enum import StrEnum


class JobStatus(StrEnum):
    SCHEDULED = "SCHEDULED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class JobPriority(StrEnum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    NORMAL = "NORMAL"
    LOW = "LOW"


class AttemptStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"
    CANCELLED = "CANCELLED"


class WorkerLivenessStatus(StrEnum):
    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    STOPPED = "STOPPED"
    UNKNOWN = "UNKNOWN"


class OperationalStatus(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    UNAVAILABLE = "UNAVAILABLE"


class JobType(StrEnum):
    WORD_COUNT = "word_count"
    SUM_NUMBERS = "sum_numbers"
    SLEEP = "sleep"
    PRIME_CALCULATION = "prime_calculation"
    SIMULATE_FAILURE = "simulate_failure"
