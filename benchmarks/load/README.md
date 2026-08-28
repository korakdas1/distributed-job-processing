# Local load benchmarks

Reproducible **local Docker Compose** performance baseline. Not an SLA. Not production capacity.

## Safety

- Isolated Compose project must start with `job-platform-load-bench`.
- Default ports: API `18002`, Postgres `15435`, Redis `16381` (checked free before start).
- `down -v` only for that project, in `finally`.
- Never prunes Docker, never `FLUSHALL`, never downs development.
- Importing these modules does not start Docker or submit jobs.

The development stack may stay running. This baseline was collected **with it running**.

## Commands

From the repository root, with `.venv` activated:

```bash
python benchmarks/load/run_benchmarks.py --quick
python benchmarks/load/run_benchmarks.py --profile all
```

Profiles: `all`, `submission`, `lightweight`, `sleep`, `cpu`, `priority`.

`--quick` is a harness smoke (small N, 1 repeat, workers 1 and 2). It is **not** the canonical baseline.

`--repeats` may be 1–5. Canonical full baseline uses 3.

Expected full runtime on this machine: on the order of **several minutes** (the recorded full run was ~282 s plus image build).

## Timing

Uses **Compose defaults** (lease 60 s, heartbeat 2/6 s). Does not apply shortened test leases.

## Results

Written to `results/`:

- `baseline.json` (`benchmark_schema_version`: 1)
- `runs.csv`
- `resource_summary.csv`

Canonical numbers in this repository are the recorded result files above. Public run IDs are descriptive labels; numeric measurements are unchanged from the recorded baseline.

## How not to interpret the numbers

- Do not say “the platform supports X jobs/s.”
- Do not treat `attempt_count=1` as exactly-once.
- Do not compare a noisy laptop run to a future cloud run without restating environment.
- Do not fail CI on a 5–20% swing; there is no performance gate yet.
