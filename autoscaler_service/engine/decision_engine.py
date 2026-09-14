"""The normative decision semantics of README section 6, as pure functions.

Nothing in this module touches AWS or the clock. Given a cluster's records and the
rules it returns the list of decisions, which makes the exact boundary conditions
(strict window edge, inclusive cooldown edge, first-rule-wins) trivially unit-testable.

Rules are the plain dicts loaded from ``rules.yaml``; :func:`validate_rules` checks
them once, up front, and logs exactly what is wrong before raising.
"""
from __future__ import annotations

import logging
from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Iterable, Iterator, Sequence

from autoscaler_service.models import ScalingDecision
from autoscaler_service.parsers import ProgressRecord, dedup_key, sort_key

logger = logging.getLogger(__name__)

Rule = dict[str, Any]

AGGREGATIONS = ("avg", "max", "min", "last")
OPERATORS = ("gt", "lt")
#: field -> (expected type, extra constraint description or None)
RULE_FIELDS: dict[str, type] = {
    "name": str,
    "metric": str,
    "aggregation": str,
    "operator": str,
    "threshold": (int, float),
    "window_minutes": int,
    "min_batches": int,
    "target_workers": int,
    "cooldown_minutes": int,
}


INT_FIELDS = ("window_minutes", "min_batches", "target_workers", "cooldown_minutes")


def _coerce_numbers(rule: Rule, label: str) -> Rule:
    """Return a copy with numeric fields coerced from lenient YAML spellings.

    PyYAML reads ``5e9`` as a *string* (YAML 1.1 needs ``5.0e+9``), and ``10.0`` as a
    float where an int is expected. Both are unambiguous, so accept them and log the
    coercion; anything that does not convert is left as-is for the type check below.
    """
    fixed = dict(rule)
    value = fixed.get("threshold")
    if isinstance(value, str):
        try:
            fixed["threshold"] = float(value)
            logger.info(f"{label}: coerced threshold {value!r} -> {fixed['threshold']}")
        except ValueError:
            pass
    for field in INT_FIELDS:
        value = fixed.get(field)
        if isinstance(value, bool):
            continue
        if isinstance(value, float) and value.is_integer():
            fixed[field] = int(value)
        elif isinstance(value, str):
            try:
                number = float(value)
            except ValueError:
                continue
            if number.is_integer():
                fixed[field] = int(number)
                logger.info(f"{label}: coerced {field} {value!r} -> {fixed[field]}")
    return fixed


def validate_rules(rules: Any) -> list[Rule]:
    """Check the rules list from ``rules.yaml`` (README section 5) and return a
    normalised copy (numeric fields coerced, see :func:`_coerce_numbers`).

    Every problem is logged with the rule's position and name; the first problem
    also raises ``ValueError`` so the service refuses to run with a rules file it
    would misinterpret — a silently skipped rule would change every decision.
    """
    if not isinstance(rules, list):
        raise ValueError(f"'rules' must be a list, got {type(rules).__name__}")
    problems: list[str] = []
    names: set[str] = set()
    normalised: list[Rule] = []
    for index, rule in enumerate(rules):
        label = f"rule #{index} ({rule.get('name', '?') if isinstance(rule, dict) else '?'})"
        if not isinstance(rule, dict):
            problems.append(f"{label}: not a mapping")
            continue
        rule = _coerce_numbers(rule, label)
        normalised.append(rule)
        for field, expected in RULE_FIELDS.items():
            value = rule.get(field)
            if value is None or isinstance(value, bool) or not isinstance(value, expected):
                problems.append(f"{label}: '{field}' must be {getattr(expected, '__name__', 'a number')}, got {value!r}")
        if rule.get("aggregation") not in AGGREGATIONS:
            problems.append(f"{label}: 'aggregation' must be one of {AGGREGATIONS}")
        if rule.get("operator") not in OPERATORS:
            problems.append(f"{label}: 'operator' must be one of {OPERATORS}")
        if isinstance(rule.get("window_minutes"), int) and rule["window_minutes"] <= 0:
            problems.append(f"{label}: 'window_minutes' must be > 0")
        if isinstance(rule.get("min_batches"), int) and rule["min_batches"] < 1:
            problems.append(f"{label}: 'min_batches' must be >= 1")
        for field in ("target_workers", "cooldown_minutes"):
            if isinstance(rule.get(field), int) and rule[field] < 0:
                problems.append(f"{label}: '{field}' must be >= 0")
        if rule.get("name") in names:
            problems.append(f"{label}: duplicate rule name")
        names.add(rule.get("name"))
    for problem in problems:
        logger.error(f"invalid rules file: {problem}")
    if problems:
        raise ValueError(f"{len(problems)} problem(s) in rules file; first: {problems[0]}")
    logger.info(f"rules validated: {', '.join(r['name'] for r in normalised)}")
    return normalised


@dataclass
class ClusterState:
    """Per-cluster state carried across evaluation points (6.4)."""

    current_workers: int
    last_decision_ts: datetime | None = None
    last_decision_ts_raw: str | None = None  # verbatim string, for trace output only


@dataclass
class StreamStats:
    """Counters filled in by :meth:`DecisionEngine.evaluate_stream`."""

    records_seen: int = 0  # every record handed in, duplicates included
    unique_records: int = 0
    evaluation_points: int = 0
    peak_held_records: int = 0  # high-water mark of records resident at once


@dataclass(frozen=True)
class RuleEvaluation:
    """Outcome of checking one rule's condition (6.3) at one evaluation point.

    ``aggregate`` is ``None`` when fewer than ``min_batches`` records carry the metric,
    in which case ``holds`` is ``False`` and no comparison was made.
    """

    holds: bool
    records_in_window: int
    records_with_metric: int
    aggregate: float | None


def dedup_and_sort(records: Iterable[ProgressRecord]) -> list[ProgressRecord]:
    """6.1: dedup by ``(id, batchId)`` (first copy wins — duplicates are identical)
    and sort ascending by ``(timestamp, batchId)``."""
    unique: dict[tuple[str, int], ProgressRecord] = {}
    for record in records:
        unique.setdefault(dedup_key(record), record)
    return sorted(unique.values(), key=sort_key)


def aggregate(values: Sequence[float], aggregation: str) -> float:
    """Combine metric values. ``values`` must be non-empty and, for ``last``, in
    ascending ``(timestamp, batchId)`` order — which :func:`dedup_and_sort` guarantees."""
    if not values:
        raise ValueError("cannot aggregate an empty window")
    if aggregation == "avg":
        return sum(values) / len(values)
    if aggregation == "max":
        return max(values)
    if aggregation == "min":
        return min(values)
    if aggregation == "last":
        return values[-1]
    raise ValueError(f"unknown aggregation {aggregation!r}")


def compare(value: float, operator: str, threshold: float) -> bool:
    """Both operators are strict (6.3 step 5)."""
    if operator == "gt":
        return value > threshold
    if operator == "lt":
        return value < threshold
    raise ValueError(f"unknown operator {operator!r}")


class DecisionEngine:
    """Evaluates a fixed, ordered list of rules against one cluster at a time."""

    def __init__(self, rules: Sequence[Rule], trace: Callable[[dict[str, Any]], None] | None = None):
        """``trace``, when given, receives one event dict per evaluation point at
        which a rule matched (debug mode); see :meth:`_trace_event` for its fields."""
        self._rules = validate_rules(list(rules))
        self._trace = trace

    @property
    def rules(self) -> list[Rule]:
        return list(self._rules)

    # -- 6.3 --------------------------------------------------------------------

    @staticmethod
    def window(
        sorted_records: Sequence[ProgressRecord],
        timestamps: Sequence[datetime],
        at: datetime,
        window_minutes: int,
    ) -> Sequence[ProgressRecord]:
        """Records with ``at - window < ts <= at``.

        ``timestamps`` is the parallel, ascending list of record timestamps so both
        edges are found with a binary search:
        * ``bisect_right(T - window)`` is the first index with ``ts > T - window``
          (a record exactly ``window`` old is excluded — strict left edge);
        * ``bisect_right(T)`` is one past the last index with ``ts <= T``.
        """
        start = bisect_right(timestamps, at - timedelta(minutes=window_minutes))
        stop = bisect_right(timestamps, at)
        return sorted_records[start:stop]

    def evaluate_rule(
        self,
        rule: Rule,
        sorted_records: Sequence[ProgressRecord],
        timestamps: Sequence[datetime],
        at: datetime,
    ) -> RuleEvaluation:
        """Apply 6.3 to one rule and report how the verdict was reached."""
        in_window = self.window(sorted_records, timestamps, at, rule["window_minutes"])
        values = [v for v in (r["metrics"].get(rule["metric"]) for r in in_window) if v is not None]
        if len(values) < rule["min_batches"]:
            return RuleEvaluation(False, len(in_window), len(values), None)
        value = aggregate(values, rule["aggregation"])
        holds = compare(value, rule["operator"], rule["threshold"])
        return RuleEvaluation(holds, len(in_window), len(values), value)

    def condition_holds(
        self,
        rule: Rule,
        sorted_records: Sequence[ProgressRecord],
        timestamps: Sequence[datetime],
        at: datetime,
    ) -> bool:
        return self.evaluate_rule(rule, sorted_records, timestamps, at).holds

    # -- 6.2 --------------------------------------------------------------------

    def matched_rule(
        self,
        sorted_records: Sequence[ProgressRecord],
        timestamps: Sequence[datetime],
        at: datetime,
    ) -> tuple[Rule, RuleEvaluation, list[str]] | None:
        """First rule in file order whose condition holds, regardless of gates.

        Returns the rule, its evaluation and the names of the rules above it that
        did *not* hold (useful in the debug trace), or ``None`` if no rule holds.
        """
        skipped: list[str] = []
        for rule in self._rules:
            evaluation = self.evaluate_rule(rule, sorted_records, timestamps, at)
            if evaluation.holds:
                return rule, evaluation, skipped
            skipped.append(rule["name"])
        return None

    # -- 6.4 --------------------------------------------------------------------

    @staticmethod
    def cooldown_until(rule: Rule, state: ClusterState) -> datetime | None:
        """Earliest time this rule may fire again, or ``None`` with no prior decision."""
        if state.last_decision_ts is None:
            return None
        return state.last_decision_ts + timedelta(minutes=rule["cooldown_minutes"])

    @classmethod
    def gate_outcome(cls, rule: Rule, state: ClusterState, at: datetime) -> str:
        """``"decision"`` if both 6.4 gates pass, else which gate blocked."""
        if rule["target_workers"] == state.current_workers:
            return "blocked_idempotence"
        until = cls.cooldown_until(rule, state)
        if until is not None and at < until:  # exactly cooldown_minutes later is allowed
            return "blocked_cooldown"
        return "decision"

    @classmethod
    def gates_allow(cls, rule: Rule, state: ClusterState, at: datetime) -> bool:
        return cls.gate_outcome(rule, state, at) == "decision"

    # -- whole cluster -----------------------------------------------------------

    @property
    def max_window_minutes(self) -> int:
        return max((r["window_minutes"] for r in self._rules), default=0)

    def evaluate_cluster(
        self,
        cluster_id: str,
        initial_workers: int,
        records: Iterable[ProgressRecord],
    ) -> list[ScalingDecision]:
        """Walk a fully buffered record set. Equivalent to :meth:`evaluate_stream`
        with a single batch; kept as the simple entry point for tests and small runs."""
        return list(self.evaluate_stream(cluster_id, initial_workers, [records]))

    def evaluate_stream(
        self,
        cluster_id: str,
        initial_workers: int,
        batches: Iterable[Iterable[ProgressRecord]],
        stats: StreamStats | None = None,
    ) -> Iterator[ScalingDecision]:
        """Walk a cluster whose records arrive in chronologically ordered *batches*
        (one per log file), holding only a bounded window in memory.

        Assumption, from the hourly rotation in README 4.4: a record is logged into
        its own file or, when the write straddles a rotation, into the *next* one —
        never later. Hence once batch *k* has been read, every record with a timestamp
        below batch *k*'s earliest timestamp is final and can be evaluated; records at
        or above it wait for batch *k+1*. At end of input everything pending is walked.

        Memory therefore holds: the pending records (about one file), plus already
        evaluated records younger than the largest ``window_minutes`` (needed as
        look-back), plus dedup keys for a little over an hour — never the whole cluster.
        Yields decisions in order; the caller writes them.
        """
        stats = stats if stats is not None else StreamStats()
        lookback = timedelta(minutes=self.max_window_minutes)
        # Rotation-overlap duplicates land in the *next* file; keep keys for a
        # comfortable margin beyond one file plus the look-back.
        dedup_horizon = lookback + timedelta(hours=2)

        state = ClusterState(current_workers=initial_workers)
        pending: list[ProgressRecord] = []  # known, not yet evaluated
        evaluated: list[ProgressRecord] = []  # sorted; kept while inside the look-back
        seen: dict[tuple[str, int], datetime] = {}  # dedup key -> timestamp, pruned by horizon

        def flush(before: datetime | None) -> Iterator[ScalingDecision]:
            nonlocal pending, evaluated
            ready = sorted((r for r in pending if before is None or r["timestamp"] < before), key=sort_key)
            if not ready:
                return
            pending = [r for r in pending if not (before is None or r["timestamp"] < before)]
            pool = sorted(evaluated + ready, key=sort_key)  # everything with ts <= any ready T
            timestamps = [r["timestamp"] for r in pool]
            for record in ready:
                stats.evaluation_points += 1
                yield from self._evaluate_point(cluster_id, record, pool, timestamps, state)
            evaluated = pool
            # prune look-back: future points have T >= before (or none remain)
            if before is not None:
                cutoff = before - lookback
                evaluated = [r for r in evaluated if r["timestamp"] > cutoff]
                for key, ts in list(seen.items()):
                    if ts <= before - dedup_horizon:
                        del seen[key]

        for batch in batches:
            batch_min: datetime | None = None
            for record in batch:
                stats.records_seen += 1
                key = dedup_key(record)
                if key in seen:
                    continue  # duplicate copies carry identical values (README 4.4)
                seen[key] = record["timestamp"]
                stats.unique_records += 1
                pending.append(record)
                if batch_min is None or record["timestamp"] < batch_min:
                    batch_min = record["timestamp"]
            stats.peak_held_records = max(stats.peak_held_records, len(pending) + len(evaluated))
            if batch_min is not None:
                yield from flush(before=batch_min)
        yield from flush(before=None)

    def _evaluate_point(
        self,
        cluster_id: str,
        record: ProgressRecord,
        pool: Sequence[ProgressRecord],
        timestamps: Sequence[datetime],
        state: ClusterState,
    ) -> Iterator[ScalingDecision]:
        """Sections 6.2-6.4 for one evaluation point; yields at most one decision."""
        at = record["timestamp"]
        match = self.matched_rule(pool, timestamps, at)
        if match is None:
            return
        rule, evaluation, skipped = match
        outcome = self.gate_outcome(rule, state, at)
        if self._trace is not None:
            self._trace(self._trace_event(cluster_id, record, rule, evaluation, skipped, state, at, outcome))
        if outcome != "decision":
            return
        decision = ScalingDecision(
            cluster_id=cluster_id,
            decision_ts=record["timestamp_raw"],
            rule_name=rule["name"],
            from_workers=state.current_workers,
            to_workers=rule["target_workers"],
        )
        state.current_workers = rule["target_workers"]
        state.last_decision_ts = at
        state.last_decision_ts_raw = record["timestamp_raw"]
        logger.info(
            f"{cluster_id}: {decision.from_workers} -> {decision.to_workers} workers "
            f"at {decision.decision_ts} (rule {rule['name']})"
        )
        yield decision

    # -- debug trace -------------------------------------------------------------

    def _trace_event(
        self,
        cluster_id: str,
        record: ProgressRecord,
        rule: Rule,
        evaluation: RuleEvaluation,
        skipped_rules: list[str],
        state: ClusterState,
        at: datetime,
        outcome: str,
    ) -> dict[str, Any]:
        """Everything needed to see *why* a matched rule did or did not fire."""
        until = self.cooldown_until(rule, state)
        return {
            "cluster_id": cluster_id,
            "evaluation_ts": record["timestamp_raw"],
            "batch_id": record["batchId"],
            "rule": rule["name"],
            "rules_not_holding_above": skipped_rules,
            "condition": {
                "metric": rule["metric"],
                "aggregation": rule["aggregation"],
                "aggregate": evaluation.aggregate,
                "operator": rule["operator"],
                "threshold": rule["threshold"],
                "comparison": f"{evaluation.aggregate} {rule['operator']} {rule['threshold']}",
                "window_minutes": rule["window_minutes"],
                "records_in_window": evaluation.records_in_window,
                "records_with_metric": evaluation.records_with_metric,
                "min_batches": rule["min_batches"],
            },
            "gates": {
                "current_workers": state.current_workers,
                "target_workers": rule["target_workers"],
                "idempotent": rule["target_workers"] == state.current_workers,
                "last_decision_ts": state.last_decision_ts_raw,
                "cooldown_minutes": rule["cooldown_minutes"],
                "cooldown_until": until.isoformat() if until else None,
                "cooldown_passed": None if until is None else at >= until,
            },
            "outcome": outcome,
        }
