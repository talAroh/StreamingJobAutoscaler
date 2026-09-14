"""DynamoDB: persist scaling decisions."""
from __future__ import annotations

import logging
import time
from typing import Any, Callable

from autoscaler_service.models import ScalingDecision
from autoscaler_service.aws_clients.base_boto_client import BaseBotoClient, BotoClientError

logger = logging.getLogger(__name__)


#: Attempts to drain ``UnprocessedItems`` from one batch before giving up.
UNPROCESSED_MAX_ATTEMPTS = 8
#: Base of the exponential backoff between those attempts (0.05, 0.1, 0.2, ... seconds).
UNPROCESSED_BACKOFF_SECONDS = 0.05


class DynamoDbClient(BaseBotoClient):
    service_name = "dynamodb"

    def __init__(self, settings, client: Any | None = None, sleep: Callable[[float], None] = time.sleep):
        super().__init__(settings, client)
        self._sleep = sleep  # injectable so tests don't wait on backoff

    @property
    def table_name(self) -> str:
        return self._settings.dynamodb_table

    def put_decision(self, decision: ScalingDecision) -> None:
        """Write one decision. ``put_item`` overwrites on an identical key, so
        re-processing a cluster after a crash is naturally idempotent."""
        self._call("put_item", TableName=self.table_name, Item=decision.to_dynamodb_item())
        logger.debug(f"wrote decision {decision.cluster_id}/{decision.decision_ts}")

    def put_decisions(self, decisions: list[ScalingDecision]) -> int:
        """Write decisions with ``batch_write_item``, ``DYNAMODB_BATCH_SIZE`` per request.

        DynamoDB may accept a batch only partially (throttling); whatever comes back in
        ``UnprocessedItems`` is re-sent with exponential backoff until the batch is fully
        durable or :data:`UNPROCESSED_MAX_ATTEMPTS` is exhausted, which raises. A batch
        must not contain the same primary key twice, so items are de-duplicated first
        with last-wins semantics — the same result sequential ``put_item`` calls give.
        Returns the number of distinct items written. Callers may retry the whole call
        after a failure: every write is an idempotent overwrite.
        """
        unique = {(d.cluster_id, d.decision_ts): d for d in decisions}
        items = [d.to_dynamodb_item() for d in unique.values()]
        size = self._settings.dynamodb_batch_size
        for start in range(0, len(items), size):
            self._write_batch(items[start : start + size])
        if len(items) != len(decisions):
            logger.warning(f"{len(decisions) - len(items)} decision(s) shared a primary key; last one kept")
        return len(items)

    def _write_batch(self, items: list[dict[str, Any]]) -> None:
        requests = [{"PutRequest": {"Item": item}} for item in items]
        for attempt in range(UNPROCESSED_MAX_ATTEMPTS):
            response = self._call("batch_write_item", RequestItems={self.table_name: requests})
            requests = response.get("UnprocessedItems", {}).get(self.table_name, [])
            if not requests:
                return
            delay = UNPROCESSED_BACKOFF_SECONDS * (2**attempt)
            logger.warning(f"{len(requests)} unprocessed item(s) in batch write; retrying in {delay:.2f}s")
            self._sleep(delay)
        raise BotoClientError(
            f"{len(requests)} item(s) still unprocessed after {UNPROCESSED_MAX_ATTEMPTS} attempts",
            code="UnprocessedItems",
            operation="batch_write_item",
        )

    def get_decision(self, cluster_id: str, decision_ts: str) -> ScalingDecision | None:
        response = self._call(
            "get_item",
            TableName=self.table_name,
            Key={"cluster_id": {"S": cluster_id}, "decision_ts": {"S": decision_ts}},
            ConsistentRead=True,
        )
        item = response.get("Item")
        return ScalingDecision.from_dynamodb_item(item) if item else None

    def list_cluster_decisions(self, cluster_id: str) -> list[ScalingDecision]:
        """All decisions of a cluster, ordered by ``decision_ts`` (the sort key)."""
        items: list[dict[str, Any]] = []
        kwargs: dict[str, Any] = {
            "TableName": self.table_name,
            "KeyConditionExpression": "cluster_id = :cid",
            "ExpressionAttributeValues": {":cid": {"S": cluster_id}},
            "ConsistentRead": True,
        }
        while True:
            response = self._call("query", **kwargs)
            items.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
        return [ScalingDecision.from_dynamodb_item(item) for item in items]

    def table_exists(self) -> bool:
        try:
            self._call("describe_table", TableName=self.table_name)
        except BotoClientError as exc:
            if exc.code == "ResourceNotFoundException":
                return False
            raise
        return True
