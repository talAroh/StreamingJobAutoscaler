"""Shared fixtures.

AWS-backed tests use moto's ``mock_aws``; they build clients with
``aws_endpoint_url=None`` so boto3 targets the (mocked) real AWS hostnames.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pytest
from moto import mock_aws

from autoscaler_service.config import Settings

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"


@pytest.fixture(autouse=True)
def _no_real_aws(monkeypatch):
    """Belt and braces: even if a test forgets mock_aws, never hit real AWS."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_PROFILE", raising=False)


@pytest.fixture
def settings() -> Settings:
    return Settings(aws_endpoint_url=None, sqs_wait_time_seconds=0, startup_retry_seconds=0)


@pytest.fixture
def aws(settings):
    """Enter moto for the duration of the test and yield the settings to use."""
    with mock_aws():
        yield settings


def ts(hh: int, mm: int, ss: int = 0, day: int = 3) -> datetime:
    return datetime(2025, 11, day, hh, mm, ss, tzinfo=timezone.utc)


def ts_raw(hh: int, mm: int, ss: int = 0, day: int = 3) -> str:
    return f"2025-11-{day:02d}T{hh:02d}:{mm:02d}:{ss:02d}.000Z"


def make_record(hh: int, mm: int, batch_id: int, *, query_id: str = "q1", ss: int = 0, **metrics: float) -> dict:
    """Build a progress-record dict at 2025-11-03 hh:mm:ss UTC with the given metrics."""
    return {
        "id": query_id,
        "batchId": batch_id,
        "timestamp": ts(hh, mm, ss),
        "timestamp_raw": ts_raw(hh, mm, ss),
        "metrics": dict(metrics),
    }


def make_rule(**overrides) -> dict:
    """Build a rule dict as loaded from rules.yaml, with sensible defaults."""
    base = dict(
        name="rule",
        metric="batchDuration",
        aggregation="avg",
        operator="gt",
        threshold=300000,
        window_minutes=15,
        min_batches=3,
        target_workers=8,
        cooldown_minutes=20,
    )
    base.update(overrides)
    return base


@pytest.fixture
def sample_log_dir() -> Path:
    if not DATA_DIR.exists():
        pytest.skip("sample data directory not present")
    return DATA_DIR / "logs"
