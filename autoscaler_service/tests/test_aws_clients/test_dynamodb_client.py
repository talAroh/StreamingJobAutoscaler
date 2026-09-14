import pytest

from autoscaler_service.aws_clients import BotoClientError, DynamoDbClient
from autoscaler_service.aws_clients.dynamodb_client import UNPROCESSED_MAX_ATTEMPTS
from autoscaler_service.config import Settings
from autoscaler_service.models import ScalingDecision


@pytest.fixture
def ddb(aws):
    client = DynamoDbClient(aws)
    client.client.create_table(
        TableName=aws.dynamodb_table,
        AttributeDefinitions=[
            {"AttributeName": "cluster_id", "AttributeType": "S"},
            {"AttributeName": "decision_ts", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "cluster_id", "KeyType": "HASH"},
            {"AttributeName": "decision_ts", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    return client


def decision(ts="2025-11-03T09:40:30.000Z", **kw) -> ScalingDecision:
    base = dict(cluster_id="cluster-aurora", decision_ts=ts, rule_name="scale_up_slow_batches", from_workers=4, to_workers=8)
    base.update(kw)
    return ScalingDecision(**base)


def test_table_exists(aws, ddb):
    """table_exists distinguishes the seeded table from a missing one."""
    assert ddb.table_exists() is True
    assert DynamoDbClient(aws.__class__(aws_endpoint_url=None, dynamodb_table="nope")).table_exists() is False


def test_put_and_get_round_trip_preserves_types(ddb):
    """A written decision reads back equal, with N/S wire types and exactly the five attributes."""
    d = decision()
    ddb.put_decision(d)
    assert ddb.get_decision(d.cluster_id, d.decision_ts) == d
    raw = ddb.client.get_item(TableName=ddb.table_name, Key={"cluster_id": {"S": d.cluster_id}, "decision_ts": {"S": d.decision_ts}})["Item"]
    assert raw["from_workers"] == {"N": "4"}
    assert raw["to_workers"] == {"N": "8"}
    assert raw["decision_ts"] == {"S": "2025-11-03T09:40:30.000Z"}
    assert set(raw) == {"cluster_id", "decision_ts", "rule_name", "from_workers", "to_workers"}


def test_put_is_idempotent_on_primary_key(ddb):
    """Writing the same decision twice leaves a single item (safe redelivery)."""
    ddb.put_decision(decision())
    ddb.put_decision(decision())
    assert len(ddb.list_cluster_decisions("cluster-aurora")) == 1


def test_list_cluster_decisions_sorted_and_scoped(ddb):
    """Querying a cluster returns only its decisions, ordered by decision_ts."""
    ddb.put_decisions([
        decision(ts="2025-11-03T10:00:00.000Z", from_workers=8, to_workers=16),
        decision(ts="2025-11-03T09:00:00.000Z"),
        decision(cluster_id="cluster-ember", ts="2025-11-03T09:30:00.000Z"),
    ])
    aurora = ddb.list_cluster_decisions("cluster-aurora")
    assert [d.decision_ts for d in aurora] == ["2025-11-03T09:00:00.000Z", "2025-11-03T10:00:00.000Z"]
    assert ddb.list_cluster_decisions("cluster-nobody") == []


def test_get_missing_returns_none(ddb):
    """get_decision returns None for an unknown key instead of raising."""
    assert ddb.get_decision("cluster-aurora", "2000-01-01T00:00:00.000Z") is None


def test_model_rejects_no_op_decision():
    """ScalingDecision refuses from_workers == to_workers."""
    with pytest.raises(ValueError):
        decision(from_workers=8, to_workers=8)


def many(n, cluster="cluster-aurora"):
    return [decision(cluster_id=cluster, ts=f"2025-11-03T{i // 3600:02d}:{(i // 60) % 60:02d}:{i % 60:02d}.000Z") for i in range(n)]


def test_put_decisions_batches_and_writes_everything(ddb):
    """60 decisions with batch size 25 → three batch_write_item calls, all 60 items durable."""
    calls = []
    real = ddb.client.batch_write_item

    def spy(**kwargs):
        calls.append(len(kwargs["RequestItems"][ddb.table_name]))
        return real(**kwargs)

    ddb.client.batch_write_item = spy
    assert ddb.put_decisions(many(60)) == 60
    assert calls == [25, 25, 10]
    assert len(ddb.list_cluster_decisions("cluster-aurora")) == 60


def test_batch_size_is_configurable(aws):
    """DYNAMODB_BATCH_SIZE controls how many items go into each request."""
    small = DynamoDbClient(Settings(aws_endpoint_url=None, dynamodb_batch_size=4))
    small.client.create_table(
        TableName=small.table_name,
        AttributeDefinitions=[{"AttributeName": "cluster_id", "AttributeType": "S"}, {"AttributeName": "decision_ts", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "cluster_id", "KeyType": "HASH"}, {"AttributeName": "decision_ts", "KeyType": "RANGE"}],
        BillingMode="PAY_PER_REQUEST",
    )
    sizes = []
    real = small.client.batch_write_item
    small.client.batch_write_item = lambda **kw: (sizes.append(len(kw["RequestItems"][small.table_name])), real(**kw))[1]
    small.put_decisions(many(10))
    assert sizes == [4, 4, 2]


def test_unprocessed_items_are_retried_with_backoff(settings):
    """Items DynamoDB hands back in UnprocessedItems are re-sent until accepted; sleeps grow exponentially."""
    sent, sleeps = [], []

    class Flaky:
        calls = 0

        def batch_write_item(self, RequestItems):
            reqs = RequestItems[settings.dynamodb_table]
            sent.append(len(reqs))
            self.calls += 1
            if self.calls <= 2:  # first two attempts accept only the first item
                return {"UnprocessedItems": {settings.dynamodb_table: reqs[1:]}}
            return {"UnprocessedItems": {}}

    client = DynamoDbClient(settings, client=Flaky(), sleep=sleeps.append)
    assert client.put_decisions(many(5)) == 5
    assert sent == [5, 4, 3]
    assert sleeps == [0.05, 0.1]


def test_gives_up_after_max_attempts(settings):
    """Persistently unprocessed items end in a BotoClientError so the message is retried later, not lost silently."""

    class NeverAccepts:
        def batch_write_item(self, RequestItems):
            return {"UnprocessedItems": RequestItems}

    client = DynamoDbClient(settings, client=NeverAccepts(), sleep=lambda s: None)
    with pytest.raises(BotoClientError) as info:
        client.put_decisions(many(3))
    assert info.value.code == "UnprocessedItems"


def test_duplicate_keys_in_one_call_keep_the_last(ddb):
    """batch_write_item rejects duplicate keys, so the client de-duplicates with put_item's last-wins semantics."""
    first = decision(to_workers=8)
    second = decision(to_workers=16)
    assert ddb.put_decisions([first, second]) == 1
    assert ddb.get_decision(first.cluster_id, first.decision_ts).to_workers == 16


def test_empty_list_writes_nothing(ddb):
    """A cluster with no decisions makes no DynamoDB call at all."""
    ddb.client.batch_write_item = lambda **kw: pytest.fail("should not be called")
    assert ddb.put_decisions([]) == 0
