"""Thin boto3 wrappers for the three AWS services the service talks to."""
from autoscaler_service.aws_clients.base_boto_client import BaseBotoClient, BotoClientError
from autoscaler_service.aws_clients.dynamodb_client import DynamoDbClient
from autoscaler_service.aws_clients.s3_client import S3Client, S3ObjectNotFound
from autoscaler_service.aws_clients.sqs_client import QueueMessage, SqsClient

__all__ = [
    "BaseBotoClient",
    "BotoClientError",
    "DynamoDbClient",
    "QueueMessage",
    "S3Client",
    "S3ObjectNotFound",
    "SqsClient",
]
