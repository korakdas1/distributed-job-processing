"""Refusal tests exercise the real safety boundary with only Docker I/O substituted."""

from __future__ import annotations

import json
import secrets
import subprocess
import tempfile
import uuid
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import SecretStr

from job_platform.core.config import Settings
from tests import disposable_infrastructure as infra
from tests.integration import process_harness
from tests.run_disruptive import run_tests


@pytest.fixture
def context(tmp_path: Path) -> infra.DisposableInfrastructure:
    run_id = "a" * 32
    return infra.DisposableInfrastructure(
        infra.PREFIX + run_id, run_id, 35432, 36379, "b" * 64, tmp_path, opted_in=True
    )


def settings_for(context: infra.DisposableInfrastructure) -> Settings:
    return Settings(
        postgres_host="127.0.0.1",
        postgres_port=context.postgres_port,
        postgres_db="job_platform_test",
        postgres_user="disruptive_test",
        postgres_password=SecretStr(context.password),
        redis_host="127.0.0.1",
        redis_port=context.redis_port,
        redis_db=15,
    )


class DockerState:
    def __init__(self, context: infra.DisposableInfrastructure) -> None:
        self.context = context
        labels = {infra.PROJECT_LABEL: context.project, infra.OWNER_LABEL: context.run_id}
        network = context.project + "_default"
        self.rows: dict[str, list[dict[str, Any]]] = {"container": [], "volume": [], "network": []}
        self.calls: list[list[str]] = []
        for service, (port, destination) in infra.SERVICES.items():
            host_port = context.postgres_port if service == "postgres" else context.redis_port
            volume = f"{context.project}_{service}_data"
            self.rows["container"].append(
                {
                    "Id": service + "-id",
                    "Name": f"/{context.project}-{service}",
                    "Config": {
                        "Labels": {
                            **labels,
                            "com.docker.compose.service": service,
                            "com.docker.compose.container-number": "1",
                        },
                        "Image": infra.IMAGES[service],
                        "Env": [
                            "POSTGRES_DB=job_platform_test",
                            "POSTGRES_USER=disruptive_test",
                            f"POSTGRES_PASSWORD={context.password}",
                        ],
                    },
                    "HostConfig": {
                        "PortBindings": {
                            f"{port}/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(host_port)}]
                        },
                        "NetworkMode": network,
                    },
                    "NetworkSettings": {"Networks": {network: {}}},
                    "Mounts": [{"Type": "volume", "Name": volume, "Destination": destination}],
                }
            )
            self.rows["volume"].append(
                {
                    "Name": volume,
                    "Labels": {**labels, "com.docker.compose.volume": service + "_data"},
                }
            )
        self.rows["network"].append(
            {
                "Name": network,
                "Id": "network-id",
                "Labels": {**labels, "com.docker.compose.network": "default"},
                "Containers": {"postgres-id": {}, "redis-id": {}},
            }
        )

    def command(self, args: list[str]) -> str:
        self.calls.append(args)
        kind = "container" if args[1] == "ps" else args[1]
        if args[1] == "ps" or args[2] == "ls":
            # Return all candidates so tests can inject inconsistent ownership metadata.
            return "\n".join(row.get("Id", row["Name"]) for row in self.rows[kind])
        if args[2] == "inspect":
            return json.dumps(self.rows[kind])
        if args[2] == "rm":
            expected = {row.get("Id", row["Name"]) for row in self.rows[kind]}
            assert set(args[3:]) - {"--force"} == expected
            self.rows[kind].clear()
            if kind == "container":
                self.rows["network"][0]["Containers"] = {}
            return ""
        assert args[2] in {"stop", "start"}
        return ""

    def mutations(self) -> list[list[str]]:
        return [args for args in self.calls if args[2] in {"stop", "start", "rm"}]


@pytest.fixture
def docker(context: infra.DisposableInfrastructure, monkeypatch: pytest.MonkeyPatch) -> DockerState:
    state = DockerState(context)
    monkeypatch.setattr(infra, "command", state.command)
    return state


@pytest.mark.parametrize(
    "action",
    [
        "stop_project_redis",
        "start_project_redis",
        "stop_project_postgres",
        "start_project_postgres",
    ],
)
def test_helper_requires_explicit_context(action: str, docker: DockerState) -> None:
    with pytest.raises(infra.UnsafeInfrastructure, match="explicit disposable"):
        getattr(process_harness, action)()
    assert not docker.calls


@pytest.mark.parametrize(
    "change",
    [
        {"opted_in": False},
        {"project": "distributed-job-processing-github"},
        {"project": "another-test-project"},
        {"project": infra.PREFIX + "wrong"},
    ],
)
def test_rejects_opt_in_or_identity(
    context: infra.DisposableInfrastructure, docker: DockerState, change: dict[str, Any]
) -> None:
    unsafe = replace(context, **change)
    with pytest.raises(infra.UnsafeInfrastructure):
        unsafe.control("stop", "redis", settings_for(context))
    assert not docker.calls


@pytest.mark.parametrize(
    "change",
    [
        {"postgres_port": 5433},
        {"redis_port": 6379},
        {"postgres_port": 12345},
        {"redis_port": 12346},
        {"postgres_host": "localhost"},
        {"redis_host": "redis"},
        {"postgres_db": "job_platform"},
        {"redis_db": 0},
        {"postgres_password": "wrong"},
    ],
)
def test_rejects_effective_targets(
    context: infra.DisposableInfrastructure, docker: DockerState, change: dict[str, Any]
) -> None:
    settings = settings_for(context).model_copy(update=change)
    if "postgres_password" in change:
        settings = settings.model_copy(update={"postgres_password": SecretStr("wrong")})
    with pytest.raises(infra.UnsafeInfrastructure):
        context.control("stop", "redis", settings)
    assert not docker.calls


@pytest.mark.parametrize(
    "fault",
    [
        "project_label",
        "owner_label",
        "volume_owner",
        "network_owner",
        "service",
        "replica",
        "ports",
        "host_binding",
        "mount",
        "foreign_network_member",
        "missing_container",
        "duplicate_container",
        "missing_volume",
        "missing_network",
        "database",
        "image",
    ],
)
def test_rejects_unproven_resources(
    context: infra.DisposableInfrastructure, docker: DockerState, fault: str
) -> None:
    row = docker.rows["container"][0]
    if fault == "project_label":
        row["Config"]["Labels"][infra.PROJECT_LABEL] = "development"
    elif fault == "owner_label":
        row["Config"]["Labels"].pop(infra.OWNER_LABEL)
    elif fault == "volume_owner":
        docker.rows["volume"][0]["Labels"][infra.OWNER_LABEL] = "foreign"
    elif fault == "network_owner":
        docker.rows["network"][0]["Labels"][infra.PROJECT_LABEL] = "foreign"
    elif fault == "service":
        row["Config"]["Labels"]["com.docker.compose.service"] = "api"
    elif fault == "replica":
        row["Config"]["Labels"]["com.docker.compose.container-number"] = "2"
    elif fault in {"ports", "host_binding"}:
        binding = row["HostConfig"]["PortBindings"]["5432/tcp"][0]
        binding["HostPort" if fault == "ports" else "HostIp"] = (
            "5433" if fault == "ports" else "0.0.0.0"
        )
    elif fault == "mount":
        row["Mounts"][0]["Name"] = "development_data"
    elif fault == "foreign_network_member":
        docker.rows["network"][0]["Containers"]["foreign-id"] = {}
    elif fault == "missing_container":
        docker.rows["container"].pop()
    elif fault == "duplicate_container":
        docker.rows["container"].append(row.copy())
    elif fault == "missing_volume":
        docker.rows["volume"].pop()
    elif fault == "missing_network":
        docker.rows["network"].clear()
    elif fault == "database":
        row["Config"]["Env"][0] = "POSTGRES_DB=job_platform"
    elif fault == "image":
        row["Config"]["Image"] = "unrelated"
    with pytest.raises(infra.UnsafeInfrastructure):
        context.control("stop", "redis", settings_for(context))
    assert not docker.mutations()
    # Missing resources can be valid during partial setup cleanup; foreign ones never are.
    if not fault.startswith("missing_"):
        with pytest.raises(infra.UnsafeInfrastructure):
            context.cleanup()
        assert not docker.mutations()


def test_validated_context_controls_only_exact_container_ids(
    context: infra.DisposableInfrastructure, docker: DockerState
) -> None:
    for service in infra.SERVICES:
        for action in ("stop", "start"):
            context.control(action, service, settings_for(context))
    assert docker.mutations() == [
        ["docker", "container", action, service + "-id"]
        for service in infra.SERVICES
        for action in ("stop", "start")
    ]
    context.cleanup()
    assert not any(docker.rows.values())
    assert json.loads((context.directory / "cleanup.json").read_text())["containers"] == 0


def test_cleanup_refuses_unexpected_volume(
    context: infra.DisposableInfrastructure, docker: DockerState
) -> None:
    docker.rows["volume"].append({"Name": "development_data", "Labels": {}})
    with pytest.raises(infra.UnsafeInfrastructure, match="Unexpected volume"):
        context.cleanup()
    assert not docker.mutations()


def test_manifest_does_not_supply_opt_in(context: infra.DisposableInfrastructure) -> None:
    with pytest.raises(infra.UnsafeInfrastructure, match="opt-in"):
        infra.load_infrastructure(context.directory / "missing.json", opted_in=False)


def test_resource_discovery_checks_project_and_run_labels(
    monkeypatch: pytest.MonkeyPatch, context: infra.DisposableInfrastructure
) -> None:
    calls: list[list[str]] = []

    def empty(args: list[str]) -> str:
        calls.append(args)
        return ""

    monkeypatch.setattr(infra, "command", empty)
    assert context.resources("container") == []
    assert calls == [
        ["docker", "ps", "-aq", "--filter", f"label={infra.PROJECT_LABEL}={context.project}"],
        ["docker", "ps", "-aq", "--filter", f"label={infra.OWNER_LABEL}={context.run_id}"],
    ]


@pytest.mark.parametrize("raises", [False, True])
def test_test_failure_still_runs_guarded_cleanup(
    context: infra.DisposableInfrastructure, monkeypatch: pytest.MonkeyPatch, raises: bool
) -> None:
    docker = DockerState(context)
    created = docker.rows
    for row in created["container"]:
        row["State"] = {"Health": {"Status": "healthy"}}
    docker.rows = {kind: [] for kind in created}

    def command(args: list[str]) -> str:
        if args[0] == "docker-compose":
            assert args[-4:] == ["up", "-d", "postgres", "redis"]
            docker.rows = created
            return ""
        return docker.command(args)

    def process(args: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        if args[:2] == ["docker", "logs"]:
            return subprocess.CompletedProcess(args, 0, "service diagnostics", "")
        assert "--run-disruptive" in args
        if raises:
            raise RuntimeError("test process failed")
        return subprocess.CompletedProcess(args, 1, "test failed\n")

    ports = iter((context.postgres_port, context.redis_port))
    monkeypatch.setattr(infra, "free_port", lambda: next(ports))
    monkeypatch.setattr(uuid, "uuid4", lambda: SimpleNamespace(hex=context.run_id))
    monkeypatch.setattr(secrets, "token_hex", lambda _: context.password)
    monkeypatch.setattr(tempfile, "mkdtemp", lambda **_: str(context.directory))
    monkeypatch.setattr(infra, "command", command)
    monkeypatch.setattr(subprocess, "run", process)
    if raises:
        with pytest.raises(RuntimeError, match="test process failed"):
            run_tests(["test_outage.py"])
    else:
        assert run_tests(["test_outage.py"]) == 1
    assert not any(docker.rows.values())
    assert (context.directory / "cleanup.json").exists()
    assert (context.directory / "postgres.log").read_text() == "service diagnostics"
    assert not (context.directory / "manifest.json").exists()
    assert not (context.directory / "compose.json").exists()
