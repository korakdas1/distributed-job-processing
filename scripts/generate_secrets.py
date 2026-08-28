#!/usr/bin/env python3
"""Generate VIEWER and OPERATOR API keys as files. Does not print secret values."""

from __future__ import annotations

import argparse
import contextlib
import os
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--directory",
        type=Path,
        default=ROOT / "secrets",
        help="Directory for secret files (default: ./secrets)",
    )
    args = parser.parse_args()
    directory: Path = args.directory
    directory.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        os.chmod(directory, 0o700)
    viewer_path = directory / "viewer_api_key"
    operator_path = directory / "operator_api_key"
    viewer_path.write_text(secrets.token_urlsafe(32) + "\n", encoding="utf-8")
    operator_path.write_text(secrets.token_urlsafe(32) + "\n", encoding="utf-8")
    # Directory is 0700. Files are 0644 so a non-root container user (uid 10001)
    # can read Docker bind mounts that keep the host file owner.
    for path in (viewer_path, operator_path):
        with contextlib.suppress(OSError):
            os.chmod(path, 0o644)
    print(f"VIEWER_KEY_FILE {viewer_path}")
    print(f"OPERATOR_KEY_FILE {operator_path}")
    print("Secret values were not printed.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"failed: {type(exc).__name__}", file=sys.stderr)
        raise SystemExit(1) from exc
