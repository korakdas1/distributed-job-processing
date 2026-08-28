#!/bin/sh
set -eu

python - <<'PY'
import os
import time

import psycopg

host = os.environ.get("POSTGRES_HOST", "postgres")
port = int(os.environ.get("POSTGRES_PORT", "5432"))
dbname = os.environ.get("POSTGRES_DB", "job_platform")
user = os.environ.get("POSTGRES_USER", "job_platform")
password = os.environ.get("POSTGRES_PASSWORD", "")
deadline = time.monotonic() + 60.0
last_error = "not attempted"
while time.monotonic() < deadline:
    try:
        with psycopg.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connect_timeout=2,
        ) as conn:
            conn.execute("SELECT 1")
        break
    except Exception as exc:
        last_error = type(exc).__name__
        time.sleep(0.5)
else:
    raise SystemExit(f"PostgreSQL was not ready before API startup: {last_error}")
PY

alembic upgrade head
exec uvicorn job_platform.api.app:app --host 0.0.0.0 --port 8000
