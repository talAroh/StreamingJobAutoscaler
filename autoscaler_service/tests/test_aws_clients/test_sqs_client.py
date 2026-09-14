import json

import pytest

from autoscaler_service.aws_clients import BotoClientError, QueueMessage, SqsClient


@pytest.fixture
def sqs(aws):
    client = SqsClient(aws)
    client.client.create_queue(QueueName=aws.sqs_queue_name)
    return client


def test_queue_url_resolved_lazily_and_cached(sqs):
    """The queue URL is looked up on first use, cached, and forgotten on reset."""
    assert sqs._queue_url is None
    url = sqs.queue_url
    assert url.endswith("/" + sqs.settings.sqs_queue_name)
    assert sqs.queue_url is url
    sqs.reset_queue_url()
    assert sqs._queue_url is None


def test_missing_queue_raises_boto_client_error(aws):
    """Resolving a queue that does not exist yet raises BotoClientError (startup before seed)."""
    client = SqsClient(aws)
    with pytest.raises(BotoClientError) as info:
        _ = client.queue_url
    assert info.value.code is not None


def test_read_parse_and_ack_round_trip(sqs):
    """Receive → parse body into a notification dict → delete leaves the queue empty."""
    body = {"bucket": "spark-driver-logs", "key": "logs/c/f.gz", "cluster_id": "c"}
    sqs.client.send_message(QueueUrl=sqs.queue_url, MessageBody=json.dumps(body))

    [message] = sqs.read_msg_from_queue()
    assert isinstance(message, QueueMessage)
    assert message.notification() == body

    sqs.ack_message_processed(message)
    assert sqs.approximate_message_count() == 0
    assert sqs.read_msg_from_queue(wait_seconds=0) == []


def test_ack_accepts_bare_receipt_handle(sqs):
    """ack_message_processed works with a receipt handle string as well as a QueueMessage."""
    sqs.client.send_message(QueueUrl=sqs.queue_url, MessageBody="{}")
    [message] = sqs.read_msg_from_queue()
    sqs.ack_message_processed(message.receipt_handle)
    assert sqs.approximate_message_count() == 0


def test_empty_queue_returns_empty_list(sqs):
    """Polling an empty queue returns [] rather than None or raising."""
    assert sqs.read_msg_from_queue(wait_seconds=0) == []


@pytest.mark.parametrize("body", ["not json", "[]", "{}", json.dumps({"bucket": "", "key": "k", "cluster_id": "c"}), json.dumps({"bucket": "b", "key": 5, "cluster_id": "c"})])
def test_invalid_body_raises_value_error(body):
    """Non-JSON, non-object, empty and blank/typed-wrong-field bodies all raise ValueError."""
    with pytest.raises(ValueError):
        QueueMessage(message_id="m", receipt_handle="r", body=body).notification()


def test_extra_fields_in_body_are_dropped():
    """Only bucket, key and cluster_id survive parsing; unknown keys are ignored."""
    body = {"bucket": "b", "key": "k", "cluster_id": "c", "extra": 1}
    assert QueueMessage(message_id="m", receipt_handle="r", body=json.dumps(body)).notification() == {"bucket": "b", "key": "k", "cluster_id": "c"}


def test_unacked_message_is_not_lost(sqs):
    """A received but not deleted message becomes visible again after the visibility timeout."""
    sqs.client.send_message(QueueUrl=sqs.queue_url, MessageBody="{}")
    sqs.client.set_queue_attributes(QueueUrl=sqs.queue_url, Attributes={"VisibilityTimeout": "0"})
    assert len(sqs.read_msg_from_queue()) == 1
    # not acked → visible again (moto honours a 0s visibility timeout immediately)
    assert len(sqs.read_msg_from_queue()) == 1


def test_batch_receive_returns_up_to_max_messages(sqs):
    """With max_messages=10 a single receive drains several queued messages at once."""
    for i in range(5):
        sqs.client.send_message(QueueUrl=sqs.queue_url, MessageBody=json.dumps({"bucket": "b", "key": f"k{i}", "cluster_id": "c"}))
    batch = sqs.read_msg_from_queue(max_messages=10, wait_seconds=0)
    assert 1 < len(batch) <= 5  # SQS may return fewer than available, never more than asked
    for m in batch:
        sqs.ack_message_processed(m)
    remaining = sqs.read_msg_from_queue(max_messages=10, wait_seconds=0)
    assert len(batch) + len(remaining) == 5


def test_receive_count_reflects_redelivery(sqs):
    """The first delivery reports receive_count 1; an unacked redelivery reports 2."""
    sqs.client.send_message(QueueUrl=sqs.queue_url, MessageBody="{}")
    sqs.client.set_queue_attributes(QueueUrl=sqs.queue_url, Attributes={"VisibilityTimeout": "0"})
    [first] = sqs.read_msg_from_queue()
    [second] = sqs.read_msg_from_queue()
    assert (first.receive_count, second.receive_count) == (1, 2)


def test_extend_visibility_changes_when_message_reappears(sqs):
    """Extending to 0 makes an in-flight message visible again at once; the default extension keeps it hidden."""
    sqs.client.send_message(QueueUrl=sqs.queue_url, MessageBody="{}")
    [m] = sqs.read_msg_from_queue()  # in flight for the queue's 30s default
    assert sqs.read_msg_from_queue(wait_seconds=0) == []
    sqs.extend_visibility(m, seconds=0)
    [again] = sqs.read_msg_from_queue(wait_seconds=0)
    assert again.message_id == m.message_id
    sqs.extend_visibility(again)  # default: settings.sqs_visibility_extension_seconds
    assert sqs.read_msg_from_queue(wait_seconds=0) == []
