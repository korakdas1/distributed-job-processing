#!/usr/bin/env python3
"""Bounded public-API demo workload for the operator dashboard.

Does not start Docker, delete data, or touch Redis/Postgres/SQL directly.
Run explicitly: python scripts/demo_dashboard_workload.py
"""

from __future__ import annotations

import argparse
import sys

import httpx

JOBS: list[dict[str, object]] = [
    {"job_type": "word_count", "payload": {"text": "hello dashboard"}, "priority": "CRITICAL"},
    {"job_type": "word_count", "payload": {"text": "second job"}, "priority": "HIGH"},
    {"job_type": "sum_numbers", "payload": {"numbers": [1, 2, 3]}, "priority": "NORMAL"},
    {"job_type": "sleep", "payload": {"seconds": 5}, "priority": "NORMAL"},
    {"job_type": "sleep", "payload": {"seconds": 5}, "priority": "LOW"},
    {
        "job_type": "word_count",
        "payload": {"text": "delayed demo"},
        "priority": "NORMAL",
        "delay_seconds": 8,
    },
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit a small dashboard demo workload.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    created: list[str] = []
    with httpx.Client(base_url=args.base_url, timeout=10.0) as client:
        for body in JOBS:
            response = client.post("/jobs", json=body)
            if response.status_code != 202:
                raise SystemExit(f"POST /jobs failed: {response.status_code} {response.text}")
            job_id = str(response.json()["id"])
            created.append(job_id)
            print(f"{job_id} {body['job_type']} {body.get('priority')}")
    print(f"submitted {len(created)} jobs")


if __name__ == "__main__":
    main()
    sys.exit(0)
