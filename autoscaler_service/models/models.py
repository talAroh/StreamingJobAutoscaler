"""ODM for the ``scaling_decisions`` DynamoDB table (README section 7).

This is the only persisted document in the system, so it is the only model here.
Inputs (SQS bodies, the manifest, the rules file, parsed progress records) are plain
dicts validated where they are consumed; problems with them are logged, not modelled.
"""
from __future__ import annotations

from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ScalingDecision(BaseModel):
    """One item of ``scaling_decisions``.

    Primary key: partition ``cluster_id`` (S), sort ``decision_ts`` (S).
    ``decision_ts`` is the triggering record's JSON timestamp string, verbatim.
    """

    model_config = ConfigDict(frozen=True)

    cluster_id: str = Field(min_length=1)
    decision_ts: str = Field(min_length=1)
    rule_name: str = Field(min_length=1)
    from_workers: int = Field(ge=0)
    to_workers: int = Field(ge=0)

    @model_validator(mode="after")
    def _must_change_workers(self) -> ScalingDecision:
        # The idempotence gate (6.4) guarantees this; enforcing it here means a bug in
        # the engine fails loudly instead of writing a no-op row.
        if self.from_workers == self.to_workers:
            raise ValueError("a scaling decision must change the worker count")
        return self

    # -- DynamoDB attribute-value mapping -------------------------------------
    # The low-level boto3 client speaks {"S": "..."} / {"N": "123"}; keeping the
    # mapping on the model means the table schema is defined in exactly one place.

    def to_dynamodb_item(self) -> dict[str, dict[str, str]]:
        return {
            "cluster_id": {"S": self.cluster_id},
            "decision_ts": {"S": self.decision_ts},
            "rule_name": {"S": self.rule_name},
            "from_workers": {"N": str(self.from_workers)},
            "to_workers": {"N": str(self.to_workers)},
        }

    @classmethod
    def from_dynamodb_item(cls, item: Mapping[str, Mapping[str, Any]]) -> ScalingDecision:
        def attr(name: str, kind: str) -> Any:
            try:
                return item[name][kind]
            except KeyError as exc:
                raise ValueError(f"DynamoDB item is missing {name!r} of type {kind!r}") from exc

        return cls(
            cluster_id=attr("cluster_id", "S"),
            decision_ts=attr("decision_ts", "S"),
            rule_name=attr("rule_name", "S"),
            from_workers=int(attr("from_workers", "N")),
            to_workers=int(attr("to_workers", "N")),
        )
