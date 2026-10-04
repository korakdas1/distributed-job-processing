"""Guarded, test-only PostgreSQL/Redis infrastructure. Never infer a development project."""

from __future__ import annotations

import json
import re
import secrets
import socket
import subprocess
import tempfile
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from job_platform.core.config import Settings

PREFIX = "job-platform-disruptive-"
OWNER_LABEL = "job-platform.test-run"
PROJECT_LABEL = "com.docker.compose.project"
SERVICES = {"postgres": (5432, "/var/lib/postgresql/data"), "redis": (6379, "/data")}
IMAGES = {"postgres": "postgres:16-alpine", "redis": "redis:7-alpine"}
ROOT = Path(__file__).resolve().parents[1]


class UnsafeInfrastructure(RuntimeError):
    """Refuse an operation whose disposable ownership cannot be proven."""


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise UnsafeInfrastructure(reason)


def command(args: list[str]) -> str:
    result = subprocess.run(args, capture_output=True, text=True, check=False, timeout=120)
    # Do not echo Docker inspect/config output: it can contain disposable credentials.
    if result.returncode:
        raise UnsafeInfrastructure(f"{args[0]} {args[1]} failed (exit {result.returncode})")
    return result.stdout


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def private_json(path: Path, value: object) -> None:
    with path.open("x", encoding="utf-8") as handle:
        path.chmod(0o600)
        json.dump(value, handle)


@dataclass(frozen=True)
class DisposableInfrastructure:
    project: str
    run_id: str
    postgres_port: int
    redis_port: int
    password: str = field(repr=False)
    directory: Path
    opted_in: bool = False

    def identity(self) -> None:
        require(self.opted_in, "Explicit disruptive-test opt-in is required")
        require(bool(re.fullmatch(r"[0-9a-f]{32}", self.run_id)), "Invalid test run identity")
        require(self.project == PREFIX + self.run_id, "Not a disposable test project")
        require(
            1024 < self.postgres_port < 65536
            and 1024 < self.redis_port < 65536
            and self.postgres_port != self.redis_port
            and self.postgres_port not in {5432, 5433, 6379}
            and self.redis_port not in {5432, 5433, 6379},
            "Invalid or development service ports",
        )
        require(len(self.password) >= 32, "Disposable credentials are required")

    def environment(self) -> dict[str, str]:
        self.identity()
        return {
            "POSTGRES_HOST": "127.0.0.1",
            "POSTGRES_PORT": str(self.postgres_port),
            "POSTGRES_DB": "job_platform_test",
            "POSTGRES_USER": "disruptive_test",
            "POSTGRES_PASSWORD": self.password,
            "REDIS_HOST": "127.0.0.1",
            "REDIS_PORT": str(self.redis_port),
            "REDIS_DB": "15",
        }

    def check_targets(self, settings: Settings) -> None:
        expected = self.environment()
        actual = {
            "POSTGRES_HOST": settings.postgres_host,
            "POSTGRES_PORT": str(settings.postgres_port),
            "POSTGRES_DB": settings.postgres_db,
            "POSTGRES_USER": settings.postgres_user,
            "POSTGRES_PASSWORD": settings.postgres_password.get_secret_value(),
            "REDIS_HOST": settings.redis_host,
            "REDIS_PORT": str(settings.redis_port),
            "REDIS_DB": str(settings.redis_db),
        }
        require(
            actual == expected,
            "Effective PostgreSQL/Redis targets do not match test infrastructure",
        )

    def resources(self, kind: str) -> list[dict[str, Any]]:
        self.identity()
        listing = ["docker", "ps", "-aq"] if kind == "container" else ["docker", kind, "ls", "-q"]
        ids: set[str] = set()
        for label in (f"{PROJECT_LABEL}={self.project}", f"{OWNER_LABEL}={self.run_id}"):
            ids.update(command([*listing, "--filter", f"label={label}"]).split())
        if not ids:
            return []
        rows: list[dict[str, Any]] = json.loads(command(["docker", kind, "inspect", *sorted(ids)]))
        return rows

    def verify(self, *, partial: bool = False) -> dict[str, list[dict[str, Any]]]:
        """Inspect live ownership, including stopped containers; partial is only for cleanup."""
        self.identity()
        found = {kind: self.resources(kind) for kind in ("container", "volume", "network")}
        expected_names = {
            "container": {f"{self.project}-{service}" for service in SERVICES},
            "volume": {f"{self.project}_{service}_data" for service in SERVICES},
            "network": {f"{self.project}_default"},
        }
        for kind, rows in found.items():
            names = {row["Name"].lstrip("/") for row in rows}
            require(len(names) == len(rows), f"Ambiguous {kind} resources")
            require(names <= expected_names[kind], f"Unexpected {kind} resources")
            require(partial or names == expected_names[kind], f"Missing {kind} resources")
            for row in rows:
                labels = (row["Config"] if kind == "container" else row).get("Labels") or {}
                require(labels.get(PROJECT_LABEL) == self.project, f"Mismatched {kind} project")
                require(labels.get(OWNER_LABEL) == self.run_id, f"Mismatched {kind} owner")
                if kind == "volume":
                    logical = row["Name"].removeprefix(f"{self.project}_")
                    require(
                        labels.get("com.docker.compose.volume") == logical, "Wrong volume identity"
                    )
                elif kind == "network":
                    require(
                        labels.get("com.docker.compose.network") == "default",
                        "Wrong network identity",
                    )
        container_ids = {row["Id"] for row in found["container"]}
        for network in found["network"]:
            require(
                set(network.get("Containers", {})) <= container_ids,
                "Network contains foreign containers",
            )
        seen_services: set[str] = set()
        for row in found["container"]:
            labels = row["Config"]["Labels"]
            service = labels.get("com.docker.compose.service")
            require(
                service in SERVICES and service not in seen_services, "Missing or ambiguous service"
            )
            seen_services.add(service)
            require(row["Name"] == f"/{self.project}-{service}", "Wrong container identity")
            require(
                labels.get("com.docker.compose.container-number") == "1",
                "Unexpected service replica",
            )
            require(row["Config"]["Image"] == IMAGES[service], "Unexpected service image")
            container_port, destination = SERVICES[service]
            host_port = self.postgres_port if service == "postgres" else self.redis_port
            bindings = {
                f"{container_port}/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(host_port)}]
            }
            require(row["HostConfig"]["PortBindings"] == bindings, "Mismatched service ports")
            require(
                row["HostConfig"]["NetworkMode"] == f"{self.project}_default", "Wrong network mode"
            )
            require(
                set(row["NetworkSettings"]["Networks"]) == {f"{self.project}_default"},
                "Wrong container network",
            )
            mounts = row["Mounts"]
            require(len(mounts) == 1, "Unexpected service mounts")
            require(
                mounts[0]["Type"] == "volume"
                and mounts[0]["Name"] == f"{self.project}_{service}_data"
                and mounts[0]["Destination"] == destination,
                "Service uses storage outside the test project",
            )
            require(
                any(v["Name"] == mounts[0]["Name"] for v in found["volume"]),
                "Missing owned storage",
            )
            require(bool(found["network"]), "Missing owned network")
            if service == "postgres":
                env = dict(entry.split("=", 1) for entry in row["Config"]["Env"] if "=" in entry)
                require(
                    env.get("POSTGRES_DB") == "job_platform_test"
                    and env.get("POSTGRES_USER") == "disruptive_test"
                    and env.get("POSTGRES_PASSWORD") == self.password,
                    "Container database or credentials do not match",
                )
        return found

    def control(self, action: str, service: str, settings: Settings) -> None:
        require(
            action in {"stop", "start"} and service in SERVICES, "Unsupported service operation"
        )
        self.check_targets(settings)
        found = self.verify()
        target = next(
            row
            for row in found["container"]
            if row["Config"]["Labels"]["com.docker.compose.service"] == service
        )
        command(["docker", "container", action, target["Id"]])
        print(f"Disposable service {action}: project={self.project} service={service}", flush=True)

    def cleanup(self) -> None:
        # Validate every resource before removing anything. Recheck between resource classes.
        for kind in ("container", "volume", "network"):
            found = self.verify(partial=True)
            ids = [row["Id"] if kind != "volume" else row["Name"] for row in found[kind]]
            if ids:
                flags = ["--force"] if kind == "container" else []
                command(["docker", kind, "rm", *flags, *ids])
        require(not any(self.verify(partial=True).values()), "Disposable resources remain")
        (self.directory / "cleanup.json").write_text(
            json.dumps({"project": self.project, "containers": 0, "volumes": 0, "networks": 0})
        )
        for name in ("compose.json", "manifest.json"):
            (self.directory / name).unlink(missing_ok=True)
        print(f"Cleanup verified: {self.project}; zero containers, volumes, networks", flush=True)


def load_infrastructure(path: Path, *, opted_in: bool) -> DisposableInfrastructure:
    require(opted_in, "Explicit disruptive-test opt-in is required")
    data = json.loads(path.read_text())
    context = DisposableInfrastructure(**data, directory=path.parent, opted_in=opted_in)
    context.identity()
    return context


def compose_spec(context: DisposableInfrastructure) -> dict[str, Any]:
    context.identity()
    labels = {OWNER_LABEL: context.run_id}
    services: dict[str, Any] = {}
    for service, (port, destination) in SERVICES.items():
        host_port = context.postgres_port if service == "postgres" else context.redis_port
        services[service] = {
            "image": IMAGES[service],
            "container_name": f"{context.project}-{service}",
            "labels": labels,
            "ports": [f"127.0.0.1:{host_port}:{port}"],
            "volumes": [f"{service}_data:{destination}"],
            "healthcheck": {
                "test": ["CMD", "pg_isready", "-U", "disruptive_test", "-d", "job_platform_test"]
                if service == "postgres"
                else ["CMD", "redis-cli", "ping"],
                "interval": "1s",
                "timeout": "2s",
                "retries": 30,
            },
        }
    services["postgres"]["environment"] = {
        "POSTGRES_DB": "job_platform_test",
        "POSTGRES_USER": "disruptive_test",
        "POSTGRES_PASSWORD": context.password,
    }
    services["redis"]["command"] = ["redis-server", "--appendonly", "yes"]
    return {
        "version": "3.8",
        "services": services,
        "volumes": {f"{service}_data": {"labels": labels} for service in SERVICES},
        "networks": {"default": {"labels": labels}},
    }


@contextmanager
def provision() -> Iterator[DisposableInfrastructure]:
    directory = Path(tempfile.mkdtemp(prefix=PREFIX))
    run_id = uuid.uuid4().hex
    pg_port, redis_port = free_port(), free_port()
    context = DisposableInfrastructure(
        PREFIX + run_id,
        run_id,
        pg_port,
        redis_port,
        secrets.token_hex(32),
        directory,
        opted_in=True,
    )
    context.identity()
    require(
        not any(context.resources(kind) for kind in ("container", "volume", "network")),
        "Project already exists; refusing to adopt it",
    )
    manifest = asdict(context)
    del manifest["directory"], manifest["opted_in"]
    private_json(directory / "manifest.json", manifest)
    private_json(directory / "compose.json", compose_spec(context))
    print(
        f"Disposable project: {context.project}; postgres={pg_port} redis={redis_port}; "
        f"diagnostics={directory}",
        flush=True,
    )
    try:
        command(
            [
                "docker-compose",
                "-f",
                str(directory / "compose.json"),
                "-p",
                context.project,
                "up",
                "-d",
                "postgres",
                "redis",
            ]
        )
        deadline = time.monotonic() + 60
        while True:
            found = context.verify()
            if all(
                row["State"].get("Health", {}).get("Status") == "healthy"
                for row in found["container"]
            ):
                break
            require(time.monotonic() < deadline, "Disposable services did not become healthy")
            time.sleep(0.25)
        yield context
    finally:
        # Capture only service logs, never docker inspect/config or environment secrets.
        try:
            for row in context.verify(partial=True)["container"]:
                service = row["Config"]["Labels"]["com.docker.compose.service"]
                logs = subprocess.run(
                    ["docker", "logs", row["Id"]], capture_output=True, text=True, timeout=15
                )
                (directory / f"{service}.log").write_text(
                    (logs.stdout + logs.stderr).replace(context.password, "[redacted]")
                )
        finally:
            context.cleanup()
