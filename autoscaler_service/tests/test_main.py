"""HTTP endpoints, exercised against a hand-built AppState (no AWS)."""
import json

import pytest
from fastapi.testclient import TestClient

from autoscaler_service.config import Settings
from autoscaler_service.engine import DecisionEngine, Scaler
from autoscaler_service.main import AppState, bootstrap, create_app, load_config_from_s3

from autoscaler_service.tests.conftest import make_rule


@pytest.fixture
def state(settings):
    return AppState(settings=settings, s3=None, sqs=None, dynamodb=None)


def test_health_and_503s_before_config_loaded(state):
    """Before the seed runs, /health reports 'starting' and /rules and /clusters answer 503."""
    with TestClient(create_app(state.settings, state, start_bootstrap=False)) as api:
        health = api.get("/health")
        assert health.status_code == 200
        assert health.json() == {"status": "starting", "workers": {"alive": 0, "configured": 1}}
        assert api.get("/rules").status_code == 503
        assert api.get("/clusters").status_code == 503


def test_endpoints_after_config_loaded(state):
    """Once rules and manifest are loaded, all three endpoints return 200 with the loaded data."""
    state.rules = [make_rule(name="r")]
    state.manifest = [{"cluster_id": "c", "initial_workers": 4, "num_log_files": 1}]
    state.scaler = Scaler(state.settings, sqs=None, s3=None, dynamodb=None, manifest=state.manifest, engine=DecisionEngine(state.rules))
    with TestClient(create_app(state.settings, state, start_bootstrap=False)) as api:
        # scaler exists but its threads were never started → reported as degraded, not ok
        assert api.get("/health").json() == {"status": "degraded", "workers": {"alive": 0, "configured": 1}}
        assert api.get("/rules").json()["rules"][0]["name"] == "r"
        clusters = api.get("/clusters").json()["clusters"]
        assert clusters[0]["cluster_id"] == "c"
        assert clusters[0]["files_received"] == 0
        assert clusters[0]["evaluated"] is False


def test_bootstrap_error_surfaces_on_health(state):
    """An invalid rules/manifest file turns /health into a 503 carrying the error message."""
    state.bootstrap_error = "invalid configuration in S3: boom"
    with TestClient(create_app(state.settings, state, start_bootstrap=False)) as api:
        response = api.get("/health")
        assert response.status_code == 503
        assert "boom" in response.json()["detail"]["error"]


def test_load_config_from_s3_returns_the_lists(aws):
    """Rules and manifest fetched from S3 come back as their plain 'rules' / 'clusters' lists."""
    from autoscaler_service.aws_clients import S3Client

    s3 = S3Client(aws)
    s3.client.create_bucket(Bucket=aws.s3_bucket)
    s3.client.put_object(Bucket=aws.s3_bucket, Key=aws.rules_key, Body="rules: []\n")
    s3.client.put_object(Bucket=aws.s3_bucket, Key=aws.manifest_key, Body=json.dumps({"clusters": [{"cluster_id": "c", "initial_workers": 1, "num_log_files": 1}]}))
    rules, manifest = load_config_from_s3(s3, aws)
    assert rules == []
    assert manifest == [{"cluster_id": "c", "initial_workers": 1, "num_log_files": 1}]


def test_load_config_from_s3_rejects_wrong_shape(aws):
    """A rules file without a top-level 'rules' key is an operator error, not a crash."""
    from autoscaler_service.aws_clients import S3Client

    s3 = S3Client(aws)
    s3.client.create_bucket(Bucket=aws.s3_bucket)
    s3.client.put_object(Bucket=aws.s3_bucket, Key=aws.rules_key, Body="- name: r\n")
    s3.client.put_object(Bucket=aws.s3_bucket, Key=aws.manifest_key, Body=json.dumps({"clusters": []}))
    with pytest.raises(ValueError, match="rules"):
        load_config_from_s3(s3, aws)


def test_health_ok_only_while_all_workers_alive(state):
    """/health is 'ok' with live workers and flips to 'degraded' once a worker thread is gone."""
    state.rules = [make_rule(name="r")]
    state.manifest = [{"cluster_id": "c", "initial_workers": 4, "num_log_files": 1}]

    class IdleSqs:
        def read_msg_from_queue(self, *a, **k):
            import time

            time.sleep(0.05)
            return []

        def reset_queue_url(self):
            pass

    state.scaler = Scaler(state.settings, sqs=IdleSqs(), s3=None, dynamodb=None, manifest=state.manifest, engine=DecisionEngine(state.rules))
    state.scaler.start()
    try:
        with TestClient(create_app(state.settings, state, start_bootstrap=False)) as api:
            assert api.get("/health").json() == {"status": "ok", "workers": {"alive": 1, "configured": 1}}
    finally:
        state.scaler.stop(timeout=5)
    with TestClient(create_app(state.settings, state, start_bootstrap=False)) as api:
        assert api.get("/health").json()["status"] == "degraded"


def test_bootstrap_reports_malformed_yaml_instead_of_dying(state):
    """Unparseable rules.yaml becomes a bootstrap_error visible on /health; the thread returns normally."""
    import yaml

    class BadYamlS3:
        def get_yaml(self, key):
            raise yaml.YAMLError("mapping values are not allowed here")

        def get_json(self, key):
            return {"clusters": []}

    state.s3 = BadYamlS3()
    bootstrap(state)  # must not raise
    assert state.bootstrap_error is not None and "mapping values" in state.bootstrap_error
    assert state.scaler is None
    with TestClient(create_app(state.settings, state, start_bootstrap=False)) as api:
        assert api.get("/health").status_code == 503
