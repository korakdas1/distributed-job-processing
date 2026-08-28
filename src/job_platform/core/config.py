"""Application settings loaded from the environment.

Extra unknown variables remain ignored. Security settings are configuration,
not a database.
"""

from functools import lru_cache
from typing import Literal, Self

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL

from job_platform.security.secrets import load_api_key, parse_allowed_hosts

MIN_API_KEY_LENGTH = 32
SecurityMode = Literal["development", "production"]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: str = "development"
    app_name: str = "job-platform"
    log_level: str = "INFO"

    api_host: str = "127.0.0.1"
    api_port: int = 8000

    postgres_host: str = "127.0.0.1"
    postgres_port: int = 5433
    postgres_db: str = "job_platform"
    postgres_user: str = "job_platform"
    postgres_password: SecretStr = SecretStr("change-me-in-local-env")

    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_db: int = Field(default=0, ge=0, le=15)
    redis_stream_critical: str = "jobs:critical"
    redis_stream_high: str = "jobs:high"
    redis_stream_normal: str = "jobs:normal"
    redis_stream_low: str = "jobs:low"
    redis_consumer_group: str = "job-workers"
    priority_weight_critical: int = Field(default=8, ge=1)
    priority_weight_high: int = Field(default=4, ge=1)
    priority_weight_normal: int = Field(default=2, ge=1)
    priority_weight_low: int = Field(default=1, ge=1)
    max_initial_delay_seconds: float = Field(default=604800, gt=0)
    worker_read_block_ms: int = Field(default=1000, ge=50, le=60_000)
    worker_reclaim_interval_ms: int = Field(default=1000, ge=50, le=60_000)
    outbox_poll_interval_ms: int = Field(default=500, ge=50, le=10_000)
    outbox_batch_size: int = Field(default=20, ge=1, le=100)

    retry_max_attempts: int = Field(default=5, ge=1)
    retry_base_delay_seconds: float = Field(default=2.0, gt=0)
    retry_max_delay_seconds: float = Field(default=300.0, gt=0)
    retry_jitter_ratio: float = Field(default=0.10, ge=0, le=1)
    redis_delayed_zset: str = "jobs:delayed"
    redis_dead_letter_stream: str = "jobs:dead"
    retry_scheduler_poll_interval_ms: int = Field(default=250, ge=50, le=10_000)
    retry_scheduler_batch_size: int = Field(default=20, ge=1, le=100)
    job_lease_timeout_seconds: int = Field(default=60, gt=0)
    payload_max_bytes: int = Field(default=32 * 1024, gt=0)
    worker_heartbeat_interval_seconds: float = Field(default=2.0, gt=0)
    worker_heartbeat_ttl_seconds: float = Field(default=6.0, gt=0)
    worker_db_heartbeat_interval_seconds: float = Field(default=10.0, gt=0)
    worker_cancellation_poll_interval_ms: int = Field(default=250, ge=50, le=5000)

    security_mode: SecurityMode = "development"
    auth_enabled: bool | None = None
    rate_limit_enabled: bool | None = None
    docs_enabled: bool | None = None
    viewer_api_key: SecretStr | None = None
    viewer_api_key_file: str | None = None
    operator_api_key: SecretStr | None = None
    operator_api_key_file: str | None = None
    rate_limit_read_per_minute: int = Field(default=600, ge=1)
    rate_limit_read_burst: int = Field(default=120, ge=1)
    rate_limit_write_per_minute: int = Field(default=30, ge=1)
    rate_limit_write_burst: int = Field(default=10, ge=1)
    allowed_hosts: str = "localhost,127.0.0.1"

    @model_validator(mode="after")
    def retry_delay_bounds(self) -> Self:
        if self.retry_max_delay_seconds < self.retry_base_delay_seconds:
            msg = "retry_max_delay_seconds must be >= retry_base_delay_seconds"
            raise ValueError(msg)
        if self.worker_heartbeat_ttl_seconds < 2 * self.worker_heartbeat_interval_seconds:
            msg = "worker_heartbeat_ttl_seconds must be >= 2 * worker_heartbeat_interval_seconds"
            raise ValueError(msg)
        if self.worker_db_heartbeat_interval_seconds < self.worker_heartbeat_interval_seconds:
            msg = (
                "worker_db_heartbeat_interval_seconds must be >= worker_heartbeat_interval_seconds"
            )
            raise ValueError(msg)
        return self

    @property
    def is_production(self) -> bool:
        return self.security_mode == "production"

    @property
    def effective_auth_enabled(self) -> bool:
        if self.auth_enabled is not None:
            return self.auth_enabled
        return self.is_production

    @property
    def effective_rate_limit_enabled(self) -> bool:
        if self.rate_limit_enabled is not None:
            return self.rate_limit_enabled
        return self.is_production

    @property
    def effective_docs_enabled(self) -> bool:
        if self.docs_enabled is not None:
            return self.docs_enabled
        return not self.is_production

    @property
    def allowed_host_list(self) -> list[str]:
        return parse_allowed_hosts(self.allowed_hosts)

    def resolved_viewer_api_key(self) -> str | None:
        return load_api_key(
            raw=self.viewer_api_key,
            file_path=self.viewer_api_key_file,
            name="viewer",
        )

    def resolved_operator_api_key(self) -> str | None:
        return load_api_key(
            raw=self.operator_api_key,
            file_path=self.operator_api_key_file,
            name="operator",
        )

    @model_validator(mode="after")
    def security_settings(self) -> Self:
        """Validate API keys only when this process has authentication enabled.

        Publisher, scheduler, and worker do not enable HTTP auth, so they can
        start without VIEWER/OPERATOR credentials. The HTTP API calls
        ``validate_api_http_security`` separately and still fails closed.
        """
        if not self.effective_auth_enabled:
            return self
        viewer = load_api_key(
            raw=self.viewer_api_key,
            file_path=self.viewer_api_key_file,
            name="viewer",
            required=True,
        )
        operator = load_api_key(
            raw=self.operator_api_key,
            file_path=self.operator_api_key_file,
            name="operator",
            required=True,
        )
        assert viewer is not None
        assert operator is not None
        if len(viewer) < MIN_API_KEY_LENGTH:
            raise ValueError(f"viewer API key must be at least {MIN_API_KEY_LENGTH} characters")
        if len(operator) < MIN_API_KEY_LENGTH:
            raise ValueError(f"operator API key must be at least {MIN_API_KEY_LENGTH} characters")
        if viewer == operator:
            raise ValueError("viewer and operator API keys must be distinct")
        return self

    @property
    def sqlalchemy_url(self) -> URL:
        """Build a SQLAlchemy URL without string-interpolating the password."""
        return URL.create(
            drivername="postgresql+psycopg",
            username=self.postgres_user,
            password=self.postgres_password.get_secret_value(),
            host=self.postgres_host,
            port=self.postgres_port,
            database=self.postgres_db,
        )


def validate_api_http_security(settings: Settings) -> None:
    """Fail-closed checks for the FastAPI process only.

    Internal publisher/scheduler/worker processes must not call this.
    """
    if settings.is_production and not settings.effective_auth_enabled:
        raise ValueError("SECURITY_MODE=production requires authentication to be enabled")
    if settings.is_production and not settings.effective_rate_limit_enabled:
        raise ValueError("SECURITY_MODE=production requires rate limiting to be enabled")
    if settings.is_production:
        hosts = parse_allowed_hosts(settings.allowed_hosts)
        if not hosts:
            raise ValueError("ALLOWED_HOSTS must be a non-empty comma-separated list")
        if any(host == "*" for host in hosts):
            raise ValueError("ALLOWED_HOSTS must not include * when SECURITY_MODE=production")
    if settings.effective_auth_enabled:
        viewer = settings.resolved_viewer_api_key()
        operator = settings.resolved_operator_api_key()
        if viewer is None or operator is None:
            raise ValueError("API authentication is enabled but API keys are not configured")


@lru_cache
def get_settings() -> Settings:
    return Settings()
