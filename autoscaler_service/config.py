"""Runtime configuration.

All defaults are the constants below; each one can be overridden by the environment
variable of the same name (see ``Settings.from_env``). The rest of the code only ever
touches a ``Settings`` instance, never ``os.environ``.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import TypeVar

# --- AWS connection (LocalStack defaults from the README) -----------------------
AWS_ENDPOINT_URL: str | None = "http://localhost:4566"  # None → real AWS endpoints
AWS_ACCESS_KEY_ID: str = "test"
AWS_SECRET_ACCESS_KEY: str = "test"
AWS_DEFAULT_REGION: str = "us-east-1"

# --- S3 ---------------------------------------------------------------------------
S3_BUCKET: str = "spark-driver-logs"
MANIFEST_KEY: str = "clusters.json"
RULES_KEY: str = "config/rules.yaml"

# --- SQS --------------------------------------------------------------------------
SQS_QUEUE_NAME: str = "driver-log-files"
SQS_WAIT_TIME_SECONDS: int = 20  # long-poll wait; SQS allows 0-20
SQS_MAX_MESSAGES: int = 1  # per receive; SQS allows 1-10
SQS_VISIBILITY_HEARTBEAT_SECONDS: int = 20  # how often a busy worker extends its message's visibility
SQS_VISIBILITY_EXTENSION_SECONDS: int = 60  # how far each heartbeat pushes the visibility timeout

# --- DynamoDB ---------------------------------------------------------------------
DYNAMODB_TABLE: str = "scaling_decisions"
DYNAMODB_BATCH_SIZE: int = 25  # items per batch_write_item; DynamoDB allows 1-25

# --- Service ----------------------------------------------------------------------
NUM_OF_WORKERS: int = 1  # SQS consumer threads
STARTUP_RETRY_SECONDS: float = 3.0  # delay between "has seed.py run yet?" probes
API_HOST: str = "0.0.0.0"
API_PORT: int = 8000
LOG_LEVEL: str = "INFO"

# --- Debugging --------------------------------------------------------------------
DEBUG_MODE: bool = False  # forces DEBUG log level and writes the trace files below
DEBUG_LOG_DIR: str = "logs"  # holds sqs_messages.log and decisions.log (JSON lines)


T = TypeVar("T", str, int, float, bool)

_TRUE = ("1", "true", "yes", "on")
_FALSE = ("0", "false", "no", "off")


def _env(name: str, default: T) -> T:
    """Read ``name`` from the environment, cast to the type of ``default``.
    Unset or blank variables fall back to ``default``."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    if isinstance(default, bool):  # bool("false") is True, so handle it by hand
        if raw.lower() in _TRUE:
            return True
        if raw.lower() in _FALSE:
            return False
        raise ValueError(f"{name} must be one of {_TRUE + _FALSE}, got {raw!r}")
    try:
        return type(default)(raw)  # str, int or float
    except ValueError as exc:
        raise ValueError(f"{name} must be {type(default).__name__}, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    """Immutable settings. Build with :meth:`from_env` in production, directly in tests."""

    aws_endpoint_url: str | None = AWS_ENDPOINT_URL
    aws_access_key_id: str = AWS_ACCESS_KEY_ID
    aws_secret_access_key: str = AWS_SECRET_ACCESS_KEY
    aws_region: str = AWS_DEFAULT_REGION

    s3_bucket: str = S3_BUCKET
    manifest_key: str = MANIFEST_KEY
    rules_key: str = RULES_KEY

    sqs_queue_name: str = SQS_QUEUE_NAME
    sqs_wait_time_seconds: int = SQS_WAIT_TIME_SECONDS
    sqs_max_messages: int = SQS_MAX_MESSAGES
    sqs_visibility_heartbeat_seconds: int = SQS_VISIBILITY_HEARTBEAT_SECONDS
    sqs_visibility_extension_seconds: int = SQS_VISIBILITY_EXTENSION_SECONDS

    dynamodb_table: str = DYNAMODB_TABLE
    dynamodb_batch_size: int = DYNAMODB_BATCH_SIZE

    num_of_workers: int = NUM_OF_WORKERS
    startup_retry_seconds: float = STARTUP_RETRY_SECONDS
    api_host: str = API_HOST
    api_port: int = API_PORT
    log_level: str = LOG_LEVEL

    debug_mode: bool = DEBUG_MODE
    debug_log_dir: str = DEBUG_LOG_DIR

    def __post_init__(self) -> None:
        if self.num_of_workers < 1:
            raise ValueError("NUM_OF_WORKERS must be >= 1")
        if not 0 <= self.sqs_wait_time_seconds <= 20:
            raise ValueError("SQS_WAIT_TIME_SECONDS must be between 0 and 20 (SQS limit)")
        if not 1 <= self.sqs_max_messages <= 10:
            raise ValueError("SQS_MAX_MESSAGES must be between 1 and 10 (SQS limit)")
        if self.sqs_visibility_heartbeat_seconds < 1:
            raise ValueError("SQS_VISIBILITY_HEARTBEAT_SECONDS must be >= 1")
        if not self.sqs_visibility_heartbeat_seconds < self.sqs_visibility_extension_seconds <= 43200:
            raise ValueError("SQS_VISIBILITY_EXTENSION_SECONDS must exceed the heartbeat interval and be <= 12h (SQS limit)")
        if not 1 <= self.dynamodb_batch_size <= 25:
            raise ValueError("DYNAMODB_BATCH_SIZE must be between 1 and 25 (DynamoDB limit)")

    @classmethod
    def from_env(cls) -> Settings:
        # An explicitly *empty* AWS_ENDPOINT_URL means "no custom endpoint" (real AWS).
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        aws_endpoint_url = AWS_ENDPOINT_URL if endpoint is None else (endpoint.strip() or None)

        return cls(
            aws_endpoint_url=aws_endpoint_url,
            aws_access_key_id=_env("AWS_ACCESS_KEY_ID", AWS_ACCESS_KEY_ID),
            aws_secret_access_key=_env("AWS_SECRET_ACCESS_KEY", AWS_SECRET_ACCESS_KEY),
            aws_region=_env("AWS_DEFAULT_REGION", AWS_DEFAULT_REGION),
            s3_bucket=_env("S3_BUCKET", S3_BUCKET),
            manifest_key=_env("MANIFEST_KEY", MANIFEST_KEY),
            rules_key=_env("RULES_KEY", RULES_KEY),
            sqs_queue_name=_env("SQS_QUEUE_NAME", SQS_QUEUE_NAME),
            sqs_wait_time_seconds=_env("SQS_WAIT_TIME_SECONDS", SQS_WAIT_TIME_SECONDS),
            sqs_max_messages=_env("SQS_MAX_MESSAGES", SQS_MAX_MESSAGES),
            sqs_visibility_heartbeat_seconds=_env("SQS_VISIBILITY_HEARTBEAT_SECONDS", SQS_VISIBILITY_HEARTBEAT_SECONDS),
            sqs_visibility_extension_seconds=_env("SQS_VISIBILITY_EXTENSION_SECONDS", SQS_VISIBILITY_EXTENSION_SECONDS),
            dynamodb_table=_env("DYNAMODB_TABLE", DYNAMODB_TABLE),
            dynamodb_batch_size=_env("DYNAMODB_BATCH_SIZE", DYNAMODB_BATCH_SIZE),
            num_of_workers=_env("NUM_OF_WORKERS", NUM_OF_WORKERS),
            startup_retry_seconds=_env("STARTUP_RETRY_SECONDS", STARTUP_RETRY_SECONDS),
            api_host=_env("API_HOST", API_HOST),
            api_port=_env("API_PORT", API_PORT),
            log_level=_env("LOG_LEVEL", LOG_LEVEL),
            debug_mode=_env("DEBUG_MODE", DEBUG_MODE),
            debug_log_dir=_env("DEBUG_LOG_DIR", DEBUG_LOG_DIR),
        )
