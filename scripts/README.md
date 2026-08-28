# Utility scripts

These helpers are for local development. They do not start Docker unless noted.

## Secure-profile helpers

`scripts/generate_secrets.py` writes VIEWER and OPERATOR API key files and prints paths, not values.

`scripts/generate_tls.sh` writes a local self-signed TLS certificate. It is not a publicly trusted certificate.

```bash
python scripts/generate_secrets.py
sh scripts/generate_tls.sh
```

Default output is gitignored `secrets/`. See the README secure-profile section.

## Dashboard demo workload

`scripts/demo_dashboard_workload.py` submits a small public-API job mix. It does not start Docker, delete data, or talk to Redis/PostgreSQL directly.

```bash
python scripts/demo_dashboard_workload.py
```

## Load benchmarks

Isolated Compose load harness lives under `benchmarks/load/`. See `benchmarks/load/README.md`.
