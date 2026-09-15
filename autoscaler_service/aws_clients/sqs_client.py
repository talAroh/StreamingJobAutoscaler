"""SQS: receive file notifications and delete them once processed."""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Tuple

from autoscaler_service.aws_clients.base_boto_client import BaseBotoClient

logger = logging.getLogger(__name__)

#: Keys every file notification must carry (README 4.2).
NOTIFICATION_FIELDS: Tuple[str, str, str] = ("bucket", "key", "cluster_id")


@dataclass(frozen=True)
class QueueMessage:
    """One received SQS message. ``receipt_handle`` is what deletes it."""

    message_id: str
    receipt_handle: str
    body: str
    receive_count: int = 1  # SQS ApproximateReceiveCount: >1 means this is a redelivery

    def notification(self) -> dict[str, str]:
        """Parse the body into ``{"bucket", "key", "cluster_id"}``.

        Raises ``ValueError`` describing what is wrong when the body is not JSON, not
        an object, or lacks one of the required non-empty string fields.
        """
        try:
            body = json.loads(self.body)
        except json.JSONDecodeError as exc:
            raise ValueError(f"body is not JSON: {exc.msg}") from exc
        if not isinstance(body, dict):
            raise ValueError("body must be a JSON object")
        missing = [f for f in NOTIFICATION_FIELDS if not isinstance(body.get(f), str) or not body[f]]
        if missing:
            raise ValueError(f"body is missing non-empty string field(s): {', '.join(missing)}")
        return {f: body[f] for f in NOTIFICATION_FIELDS}


class SqsClient(BaseBotoClient):
    service_name = "sqs"

    def __init__(self, settings, client: Any | None = None):
        super().__init__(settings, client)
        self._queue_url: str | None = None

    @property
    def queue_url(self) -> str:
        """Resolve the queue URL from its name on first use and cache it.

        Resolving lazily matters at startup: the container may come up before
        ``seed.py`` has created the queue, and the seed recreates it on every run.
        """
        if self._queue_url is None:
            response = self._call("get_queue_url", QueueName=self._settings.sqs_queue_name)
            self._queue_url = response["QueueUrl"]
        return self._queue_url

    def reset_queue_url(self) -> None:
        """Forget the cached URL (e.g. after the queue was recreated by a reseed)."""
        self._queue_url = None

    def read_msg_from_queue(
        self, max_messages: int | None = None, wait_seconds: int | None = None
    ) -> list[QueueMessage]:
        """Long-poll the queue. Returns ``[]`` when nothing arrived within the wait."""
        response = self._call(
            "receive_message",
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages or self._settings.sqs_max_messages,
            WaitTimeSeconds=self._settings.sqs_wait_time_seconds
            if wait_seconds is None
            else wait_seconds,
            AttributeNames=["ApproximateReceiveCount"],
        )
        messages = [
            QueueMessage(
                message_id=m["MessageId"],
                receipt_handle=m["ReceiptHandle"],
                body=m["Body"],
                receive_count=int(m.get("Attributes", {}).get("ApproximateReceiveCount", 1)),
            )
            for m in response.get("Messages", [])
        ]
        if messages:
            logger.debug(f"received {len(messages)} message(s) from {self.queue_url}")
        return messages

    def ack_message_processed(self, message: QueueMessage | str) -> None:
        """Delete a message so SQS never redelivers it. Accepts a message or a
        bare receipt handle."""
        receipt_handle = message if isinstance(message, str) else message.receipt_handle
        self._call("delete_message", QueueUrl=self.queue_url, ReceiptHandle=receipt_handle)

    def extend_visibility(self, message: QueueMessage | str, seconds: int | None = None) -> None:
        """Push the message's visibility timeout ``seconds`` into the future so a slow
        consumer keeps exclusive ownership instead of triggering a redelivery."""
        receipt_handle = message if isinstance(message, str) else message.receipt_handle
        self._call(
            "change_message_visibility",
            QueueUrl=self.queue_url,
            ReceiptHandle=receipt_handle,
            VisibilityTimeout=seconds if seconds is not None else self._settings.sqs_visibility_extension_seconds,
        )

    def approximate_message_count(self) -> int:
        """Visible messages still waiting. Used by the health endpoint."""
        response = self._call(
            "get_queue_attributes",
            QueueUrl=self.queue_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        return int(response["Attributes"]["ApproximateNumberOfMessages"])


__all__ = ["QueueMessage", "SqsClient"]
