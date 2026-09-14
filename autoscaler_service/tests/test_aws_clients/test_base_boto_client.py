import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from botocore.stub import Stubber

from autoscaler_service.aws_clients import BaseBotoClient, BotoClientError
from autoscaler_service.config import Settings


class SqsProbe(BaseBotoClient):
    service_name = "sqs"


class Nameless(BaseBotoClient):
    pass


def test_requires_service_name(settings):
    """A subclass without service_name cannot be instantiated."""
    with pytest.raises(TypeError):
        Nameless(settings)


def test_builds_client_from_settings():
    """Endpoint, region and a read timeout above the SQS long-poll wait come from Settings."""
    s = Settings(aws_endpoint_url="http://example.invalid:4566", aws_region="eu-west-1", sqs_wait_time_seconds=20)
    probe = SqsProbe(s)
    assert probe.client.meta.endpoint_url == "http://example.invalid:4566"
    assert probe.client.meta.region_name == "eu-west-1"
    assert probe.settings is s
    # read timeout must exceed the long-poll wait or every receive would time out
    assert probe.client.meta.config.read_timeout >= s.sqs_wait_time_seconds + 10


def test_injected_client_is_used(settings):
    """A pre-built client passed in is used as-is (the hook tests and fakes rely on)."""
    fake = object()
    assert SqsProbe(settings, client=fake).client is fake


def test_client_error_is_translated_with_code(settings):
    """A botocore ClientError becomes BotoClientError carrying the AWS error code and operation."""
    raw = boto3.client("sqs", region_name="us-east-1", aws_access_key_id="x", aws_secret_access_key="y")
    probe = SqsProbe(settings, client=raw)
    with Stubber(raw) as stub:
        stub.add_client_error("get_queue_url", service_error_code="AWS.SimpleQueueService.NonExistentQueue", http_status_code=400)
        with pytest.raises(BotoClientError) as info:
            probe._call("get_queue_url", QueueName="nope")
    assert info.value.code == "AWS.SimpleQueueService.NonExistentQueue"
    assert info.value.operation == "get_queue_url"
    assert isinstance(info.value.__cause__, ClientError)


def test_botocore_error_is_translated_without_code(settings):
    """Connection-level BotoCoreErrors become BotoClientError with no code but a useful message."""
    class Broken:
        def receive_message(self, **kwargs):
            raise EndpointConnectionError(endpoint_url="http://localstack:4566")

    probe = SqsProbe(settings, client=Broken())
    with pytest.raises(BotoClientError) as info:
        probe._call("receive_message", QueueUrl="q")
    assert info.value.code is None
    assert "localstack" in str(info.value)


def test_error_code_helper():
    """error_code() extracts the AWS code and returns None when the response has none."""
    exc = ClientError({"Error": {"Code": "NoSuchKey", "Message": "m"}}, "GetObject")
    assert BaseBotoClient.error_code(exc) == "NoSuchKey"
    assert BaseBotoClient.error_code(ClientError({}, "GetObject")) is None
