import pytest

from autoscaler_service.config import Settings


def test_defaults_match_readme():
    """Default settings equal the LocalStack names and paths given in the README."""
    s = Settings()
    assert s.aws_endpoint_url == "http://localhost:4566"
    assert (s.s3_bucket, s.sqs_queue_name, s.dynamodb_table) == ("spark-driver-logs", "driver-log-files", "scaling_decisions")
    assert (s.rules_key, s.manifest_key) == ("config/rules.yaml", "clusters.json")
    assert s.num_of_workers == 1
    assert s.api_port == 8000


def test_from_env_overrides(monkeypatch):
    """Environment variables override the constants and are cast to the right type."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://localstack:4566")
    monkeypatch.setenv("NUM_OF_WORKERS", "3")
    monkeypatch.setenv("RULES_KEY", "other/rules.yaml")
    monkeypatch.setenv("API_PORT", "9000")
    s = Settings.from_env()
    assert s.aws_endpoint_url == "http://localstack:4566"
    assert s.num_of_workers == 3
    assert s.rules_key == "other/rules.yaml"
    assert s.api_port == 9000


def test_empty_endpoint_means_none(monkeypatch):
    """A blank AWS_ENDPOINT_URL disables the custom endpoint (real AWS)."""
    monkeypatch.setenv("AWS_ENDPOINT_URL", "")
    assert Settings.from_env().aws_endpoint_url is None


@pytest.mark.parametrize(
    "kwargs",
    [
        dict(num_of_workers=0),
        dict(sqs_wait_time_seconds=21),
        dict(sqs_max_messages=11),
        dict(dynamodb_batch_size=0),
        dict(dynamodb_batch_size=26),
        dict(sqs_visibility_heartbeat_seconds=0),
        dict(sqs_visibility_heartbeat_seconds=60, sqs_visibility_extension_seconds=60),  # must exceed heartbeat
        dict(sqs_visibility_extension_seconds=43201),
    ],
)
def test_invalid_values_rejected(kwargs):
    """Out-of-range worker count and SQS limits are rejected at construction."""
    with pytest.raises(ValueError):
        Settings(**kwargs)


def test_non_integer_env_is_an_error(monkeypatch):
    """A non-numeric value for an integer setting raises a ValueError naming the variable."""
    monkeypatch.setenv("NUM_OF_WORKERS", "many")
    with pytest.raises(ValueError):
        Settings.from_env()


def test_debug_mode_defaults_off():
    """Debug mode is opt-in."""
    assert Settings().debug_mode is False
    assert Settings().debug_log_dir == "logs"


@pytest.mark.parametrize("raw,expected", [("true", True), ("1", True), ("YES", True), ("on", True), ("false", False), ("0", False), ("no", False), ("Off", False)])
def test_debug_mode_bool_parsing(monkeypatch, raw, expected):
    """Common truthy/falsy spellings are accepted case-insensitively."""
    monkeypatch.setenv("DEBUG_MODE", raw)
    assert Settings.from_env().debug_mode is expected


def test_debug_mode_rejects_garbage(monkeypatch):
    """bool('maybe') would be True; we reject anything that is not a known spelling."""
    monkeypatch.setenv("DEBUG_MODE", "maybe")
    with pytest.raises(ValueError, match="DEBUG_MODE"):
        Settings.from_env()


def test_batch_and_heartbeat_defaults():
    """Batch size defaults to DynamoDB's maximum; the heartbeat fires well inside the extension it grants."""
    s = Settings()
    assert s.dynamodb_batch_size == 25
    assert s.sqs_visibility_heartbeat_seconds < s.sqs_visibility_extension_seconds
