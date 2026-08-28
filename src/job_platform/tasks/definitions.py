"""Allowlisted task types and payload contracts. Handlers live in tasks.handlers."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from job_platform.core.enums import JobType
from job_platform.core.errors import AppError

_EXTRA_FORBID = ConfigDict(extra="forbid")


class WordCountPayload(BaseModel):
    model_config = _EXTRA_FORBID

    text: str


class SumNumbersPayload(BaseModel):
    model_config = _EXTRA_FORBID

    numbers: list[float]

    @field_validator("numbers")
    @classmethod
    def numbers_must_be_finite(cls, value: list[float]) -> list[float]:
        if any(not math.isfinite(item) for item in value):
            raise ValueError("numbers must contain only finite values")
        return value


class SleepPayload(BaseModel):
    model_config = _EXTRA_FORBID

    seconds: float = Field(ge=0, le=30)

    @field_validator("seconds")
    @classmethod
    def seconds_must_be_finite(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("seconds must be finite")
        return value


class PrimeCalculationPayload(BaseModel):
    model_config = _EXTRA_FORBID

    limit: int = Field(ge=2, le=100_000)


class SimulateFailurePayload(BaseModel):
    model_config = _EXTRA_FORBID

    mode: Literal["retryable", "non_retryable"] = "non_retryable"


PAYLOAD_MODELS: dict[JobType, type[BaseModel]] = {
    JobType.WORD_COUNT: WordCountPayload,
    JobType.SUM_NUMBERS: SumNumbersPayload,
    JobType.SLEEP: SleepPayload,
    JobType.PRIME_CALCULATION: PrimeCalculationPayload,
    JobType.SIMULATE_FAILURE: SimulateFailurePayload,
}


def parse_job_type(value: str) -> JobType:
    try:
        return JobType(value)
    except ValueError as exc:
        raise AppError(
            "UNKNOWN_JOB_TYPE",
            f"Unknown job type {value!r}.",
            status_code=422,
            details={"job_type": value},
        ) from exc


def validate_payload(job_type: JobType, payload: Mapping[str, Any]) -> dict[str, Any]:
    model = PAYLOAD_MODELS[job_type]
    try:
        parsed = model.model_validate(payload)
    except ValidationError as exc:
        raise AppError(
            "INVALID_PAYLOAD",
            f"Payload is invalid for job type {job_type}.",
            status_code=422,
            details={"job_type": job_type.value, "reason": str(exc)},
        ) from exc
    return parsed.model_dump(mode="json")


def encoded_payload_size(payload: Mapping[str, Any]) -> int:
    return len(json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def assert_payload_size(payload: Mapping[str, Any], max_bytes: int) -> None:
    size = encoded_payload_size(payload)
    if size > max_bytes:
        raise AppError(
            "PAYLOAD_TOO_LARGE",
            f"Payload exceeds the maximum size of {max_bytes} bytes.",
            status_code=422,
            details={"size_bytes": size, "max_bytes": max_bytes},
        )
