"""Boundary conditions of README section 6, one test per sentence that matters."""
import json

import pytest

from autoscaler_service.engine import DecisionEngine, StreamStats, aggregate, dedup_and_sort, validate_rules
from autoscaler_service.engine.decision_engine import ClusterState, compare
from autoscaler_service.parsers import SparkLogParser

from autoscaler_service.tests.conftest import make_record, make_rule, ts, ts_raw


def progress_line(prefix: str, doc: dict) -> str:
    """A single-line ProgressReporter entry whose log-line prefix is chosen by the caller."""
    return f"{prefix} INFO ProgressReporter: Streaming query made progress: {json.dumps(doc)}"


def evaluate_with_trace(engine_rules: list[dict], initial_workers: int, records) -> tuple[list, dict[str, dict]]:
    """Run the walk in debug mode; return the decisions and the trace events keyed by evaluation_ts."""
    events: list[dict] = []
    decisions = DecisionEngine(engine_rules, trace=events.append).evaluate_cluster("c", initial_workers, records)
    by_ts = {e["evaluation_ts"]: e for e in events}
    assert len(by_ts) == len(events), "one trace event per evaluation point"
    return decisions, by_ts


class TestDedupAndSort:
    def test_dedups_by_id_and_batch_and_sorts_by_timestamp_then_batch(self):
        """README 6.1: records are unique per (id, batchId) and ordered by (timestamp, batchId)."""
        r1 = make_record(9, 10, 3)
        r1_dup = make_record(9, 10, 3)
        r2 = make_record(9, 8, 2)
        r3 = make_record(9, 10, 1)  # same timestamp as r1, lower batchId → before it
        other_query = make_record(9, 10, 3, query_id="q2")
        result = dedup_and_sort([r1, r1_dup, r2, r3, other_query])
        assert [(r["id"], r["batchId"]) for r in result] == [("q1", 2), ("q1", 1), ("q1", 3), ("q2", 3)]

    def test_first_copy_wins(self):
        """README 6.1: when duplicates differ, the first one seen is kept."""
        first = make_record(9, 10, 3, batchDuration=1)
        second = make_record(9, 10, 3, batchDuration=2)
        assert dedup_and_sort([first, second])[0]["metrics"]["batchDuration"] == 1


class TestAggregate:
    def test_all_aggregations(self):
        """README 6.3 step 4: avg, max, min and last compute the expected values."""
        values = [1.0, 5.0, 3.0]
        assert aggregate(values, "avg") == 3.0
        assert aggregate(values, "max") == 5.0
        assert aggregate(values, "min") == 1.0
        assert aggregate(values, "last") == 3.0

    def test_empty_raises(self):
        """README 6.3 step 3 guard: aggregating an empty window is a programming error, not a silent 0."""
        with pytest.raises(ValueError):
            aggregate([], "avg")

    def test_operators_are_strict(self):
        """README 6.3 step 5: gt and lt are strict, equality never satisfies a rule."""
        assert compare(5, "gt", 5) is False
        assert compare(5, "lt", 5) is False
        assert compare(6, "gt", 5) is True
        assert compare(4, "lt", 5) is True


class TestWindow:
    """Records every 2 minutes from 09:00; evaluate at T=09:20 with a 10-minute window."""

    records = [make_record(9, m, m // 2, batchDuration=1) for m in range(0, 22, 2)]
    timestamps = [r["timestamp"] for r in records]

    def test_left_edge_is_exclusive_right_edge_inclusive(self):
        """README 6.3 step 1: a record exactly window_minutes old is outside the window; the record at T is inside."""
        in_window = DecisionEngine.window(self.records, self.timestamps, ts(9, 20), 10)
        minutes = [r["timestamp"].minute for r in in_window]
        assert minutes == [12, 14, 16, 18, 20]  # 09:10 is exactly 10 min old → excluded

    def test_future_records_are_excluded(self):
        """README 6.3 step 1: records after the evaluation point (ts > T) never enter the window."""
        in_window = DecisionEngine.window(self.records, self.timestamps, ts(9, 10), 30)
        assert max(r["timestamp"] for r in in_window) == ts(9, 10)


class TestConditionHolds:
    def test_min_batches_counts_only_records_carrying_the_metric(self):
        """README 6.3 steps 2-3: records missing the rule's metric do not count toward min_batches."""
        rule = make_rule(min_batches=3, window_minutes=10, threshold=100, operator="gt")
        records = [
            make_record(9, 2, 1, batchDuration=500),
            make_record(9, 4, 2),  # no batchDuration
            make_record(9, 6, 3, batchDuration=500),
            make_record(9, 8, 4),  # no batchDuration
        ]
        engine = DecisionEngine([rule])
        timestamps = [r["timestamp"] for r in records]
        assert engine.condition_holds(rule, records, timestamps, ts(9, 8)) is False
        records.append(make_record(9, 9, 5, batchDuration=500))
        timestamps = [r["timestamp"] for r in records]
        assert engine.condition_holds(rule, records, timestamps, ts(9, 9)) is True

    def test_last_uses_greatest_timestamp_batch_in_window(self):
        """README 6.3 step 4: 'last' takes the value of the newest record in the window, not the max value."""
        rule = make_rule(metric="source.numBytesOutstanding", aggregation="last", min_batches=2, threshold=10, operator="gt")
        records = [
            make_record(9, 0, 1, **{"source.numBytesOutstanding": 100}),
            make_record(9, 2, 2, **{"source.numBytesOutstanding": 5}),  # last → 5, not > 10
        ]
        engine = DecisionEngine([rule])
        assert engine.condition_holds(rule, records, [r["timestamp"] for r in records], ts(9, 2)) is False

    def test_last_breaks_equal_timestamps_by_batch_id(self):
        """README 6.3 step 4: 'last' is the record with the greatest (timestamp, batchId), so with equal
        timestamps the higher batchId wins regardless of the order the records arrived in."""
        rule = make_rule(metric="numInputRows", aggregation="last", min_batches=2, threshold=10, operator="gt", window_minutes=5)
        arrived = [
            make_record(9, 0, 2, numInputRows=5),  # higher batchId, arrives first → this is 'last'
            make_record(9, 0, 1, numInputRows=100),
        ]
        records = dedup_and_sort(arrived)
        timestamps = [r["timestamp"] for r in records]
        engine = DecisionEngine([rule])
        assert engine.evaluate_rule(rule, records, timestamps, ts(9, 0)).aggregate == 5
        assert engine.condition_holds(rule, records, timestamps, ts(9, 0)) is False

    def test_aggregate_uses_only_values_inside_the_window(self):
        """README 6.3 steps 1 and 4: a record that has just fallen out of the window no longer
        contributes to the aggregate, even though it still carries the metric."""
        rule = make_rule(aggregation="max", operator="gt", threshold=100, min_batches=1, window_minutes=10)
        records = [make_record(9, 0, 1, batchDuration=500), make_record(9, 10, 2, batchDuration=50)]
        timestamps = [r["timestamp"] for r in records]
        engine = DecisionEngine([rule])
        # at 09:10 the 09:00 record is exactly 10 minutes old → outside → max is 50
        assert engine.evaluate_rule(rule, records, timestamps, ts(9, 10)).aggregate == 50
        assert engine.condition_holds(rule, records, timestamps, ts(9, 10)) is False
        assert engine.condition_holds(rule, records, timestamps, ts(9, 9)) is True

    def test_min_aggregation_with_lt(self):
        """README 6.3 steps 4-5: a max/lt rule holds when every value in the window is below the threshold."""
        rule = make_rule(metric="numInputRows", aggregation="max", operator="lt", threshold=1000, min_batches=2, window_minutes=30)
        records = [make_record(9, 0, 1, numInputRows=10), make_record(9, 2, 2, numInputRows=999)]
        engine = DecisionEngine([rule])
        assert engine.condition_holds(rule, records, [r["timestamp"] for r in records], ts(9, 2)) is True


class TestGates:
    rule = make_rule(target_workers=8, cooldown_minutes=20)

    def test_idempotence(self):
        """README 6.4 idempotence gate: a rule targeting the current worker count produces no decision."""
        assert DecisionEngine.gates_allow(self.rule, ClusterState(current_workers=8), ts(9, 0)) is False
        assert DecisionEngine.gates_allow(self.rule, ClusterState(current_workers=4), ts(9, 0)) is True

    def test_cooldown_is_strict_less_than(self):
        """README 6.4 cooldown gate: blocks up to, but not at, exactly cooldown_minutes after the last decision."""
        state = ClusterState(current_workers=4, last_decision_ts=ts(9, 40))
        assert DecisionEngine.gates_allow(self.rule, state, ts(9, 59, 59)) is False
        assert DecisionEngine.gates_allow(self.rule, state, ts(10, 0)) is True  # exactly 20 min → fires


    def test_no_prior_decision_means_no_cooldown(self):
        """README 6.4: last_decision_ts starts unset, so the cooldown gate cannot block the first decision."""
        assert DecisionEngine.cooldown_until(self.rule, ClusterState(current_workers=4)) is None
        assert DecisionEngine.gate_outcome(self.rule, ClusterState(current_workers=4), ts(9, 0)) == "decision"

    def test_state_starts_from_manifest_and_updates_only_on_a_decision(self):
        """README 6.4: current_workers starts at the manifest's initial_workers and last_decision_ts
        unset; both change when a decision is produced and stay put when a matched rule is gated."""
        # 1-minute windows: each evaluation point sees only its own record.
        up = make_rule(name="up", metric="batchDuration", operator="gt", threshold=100, min_batches=1, window_minutes=1, target_workers=8, cooldown_minutes=10)
        down = make_rule(name="down", metric="numInputRows", operator="lt", threshold=100, min_batches=1, window_minutes=1, target_workers=2, cooldown_minutes=10)
        records = [
            make_record(9, 0, 1, batchDuration=500),  # up: 4 → 8
            make_record(9, 5, 2, numInputRows=1),  # down matches, cooldown blocks
            make_record(9, 7, 3, batchDuration=500),  # up matches, idempotence blocks
            make_record(9, 10, 4, numInputRows=1),  # down: exactly 10 min after 09:00 → 8 → 2
        ]
        decisions, events = evaluate_with_trace([up, down], 4, records)

        first = events[ts_raw(9, 0)]["gates"]
        assert (first["current_workers"], first["last_decision_ts"], first["cooldown_until"]) == (4, None, None)
        assert events[ts_raw(9, 0)]["outcome"] == "decision"

        blocked_cooldown = events[ts_raw(9, 5)]
        assert blocked_cooldown["outcome"] == "blocked_cooldown"
        assert (blocked_cooldown["gates"]["current_workers"], blocked_cooldown["gates"]["last_decision_ts"]) == (8, ts_raw(9, 0))

        blocked_idem = events[ts_raw(9, 7)]
        assert blocked_idem["outcome"] == "blocked_idempotence"
        # neither blocked match touched the state
        assert (blocked_idem["gates"]["current_workers"], blocked_idem["gates"]["last_decision_ts"]) == (8, ts_raw(9, 0))

        assert events[ts_raw(9, 10)]["outcome"] == "decision"
        assert [(d.from_workers, d.to_workers) for d in decisions] == [(4, 8), (8, 2)]


class TestWorkedExample:
    """README 6.5, reproduced: A above B, cluster starts at 4, batches every 2 minutes."""

    rule_a = make_rule(name="A", metric="batchDuration", aggregation="avg", operator="gt", threshold=300000, window_minutes=15, min_batches=3, target_workers=8, cooldown_minutes=20)
    rule_b = make_rule(name="B", metric="numInputRows", aggregation="max", operator="lt", threshold=1000, window_minutes=30, min_batches=5, target_workers=2, cooldown_minutes=60)

    def records(self):
        # 09:26 .. 09:58, slow batches throughout and tiny input → both A and B would hold.
        return [
            make_record(9, m, 100 + m, batchDuration=344000, numInputRows=10)
            for m in range(26, 60, 2)
        ]

    def test_first_decision_then_idempotence_then_cooldown(self):
        """README 6.5 (and 6.4): rule A fires once at 09:30, then idempotence gates it at every later point."""
        engine = DecisionEngine([self.rule_a, self.rule_b])
        decisions = engine.evaluate_cluster("c", 4, self.records())
        # A first holds at 09:30 (3 records in (09:15, 09:30]); B never gets a look
        # because A is matched at every point and idempotence/cooldown gate it.
        assert [(d.decision_ts, d.rule_name, d.from_workers, d.to_workers) for d in decisions] == [
            ("2025-11-03T09:30:00.000Z", "A", 4, 8),
        ]

    def test_lower_rule_not_considered_when_upper_matched_but_gated(self):
        """README 6.2: a gated upper rule still shadows the lower rule (first match wins, gates or not)."""
        engine = DecisionEngine([self.rule_a, self.rule_b])
        decisions = engine.evaluate_cluster("c", 8, self.records())  # already at A's target
        assert decisions == []  # A matches everywhere and is gated; B is never evaluated

    def test_lower_rule_fires_when_upper_does_not_hold(self):
        """README 6.2: when the upper rule's condition fails, the lower rule can fire on its 5th record."""
        engine = DecisionEngine([self.rule_a, self.rule_b])
        fast = [make_record(9, m, 100 + m, batchDuration=1000, numInputRows=10) for m in range(26, 60, 2)]
        decisions = engine.evaluate_cluster("c", 8, fast)
        assert len(decisions) == 1
        assert decisions[0].rule_name == "B"
        assert (decisions[0].from_workers, decisions[0].to_workers) == (8, 2)
        assert decisions[0].decision_ts == "2025-11-03T09:34:00.000Z"  # 5th record


    def test_readme_timeline_reproduced_exactly(self):
        """README 6.5, step by step with the README's own numbers: A is matched first at 09:40 with 8
        records averaging 344000 and fires 4 → 8; 09:42 is gated by idempotence; at 09:56 the cooldown
        has not passed; at 10:00 it has (exactly 20 min) but idempotence still blocks. B never matches."""
        # 09:26..09:38: avg is exactly 300000 → strict gt fails (6.3 step 5), so A does not hold yet.
        # 09:40: (7 * 300000 + 652000) / 8 == 344000, the README's figure.
        # Input volume is high throughout, so B's condition never holds.
        records = [make_record(9, m, 100 + m, batchDuration=300000, numInputRows=50000) for m in range(26, 40, 2)]
        records.append(make_record(9, 40, 140, batchDuration=652000, numInputRows=50000))
        records += [make_record(9, m, 100 + m, batchDuration=344000, numInputRows=50000) for m in range(42, 60, 2)]
        records.append(make_record(10, 0, 160, batchDuration=344000, numInputRows=50000))

        decisions, events = evaluate_with_trace([self.rule_a, self.rule_b], 4, records)

        assert [(d.decision_ts, d.rule_name, d.from_workers, d.to_workers) for d in decisions] == [
            (ts_raw(9, 40), "A", 4, 8),
        ]
        assert min(events) == ts_raw(9, 40)  # no rule matched at any earlier point
        assert {e["rule"] for e in events.values()} == {"A"}  # B is never the matched rule

        at_0940 = events[ts_raw(9, 40)]
        assert at_0940["outcome"] == "decision"
        assert (at_0940["condition"]["records_with_metric"], at_0940["condition"]["aggregate"]) == (8, 344000)
        assert at_0940["gates"]["last_decision_ts"] is None

        at_0942 = events[ts_raw(9, 42)]
        assert at_0942["outcome"] == "blocked_idempotence"
        assert (at_0942["gates"]["current_workers"], at_0942["gates"]["target_workers"]) == (8, 8)

        at_0956 = events[ts_raw(9, 56)]
        assert at_0956["outcome"] != "decision"
        assert at_0956["gates"]["cooldown_passed"] is False  # 09:56 < 09:40 + 20min

        at_1000 = events[ts_raw(10, 0)]
        assert at_1000["outcome"] != "decision"
        assert at_1000["gates"]["cooldown_passed"] is True  # 10:00 == 09:40 + 20min
        assert at_1000["gates"]["idempotent"] is True  # ...but 8 == 8, so still nothing


class TestEvaluationPoints:
    def test_record_missing_the_metric_is_still_an_evaluation_point(self):
        """README 6.2: a record that lacks the rule's metric is still an evaluation point; the
        decision is stamped with *its* timestamp when the window ending there satisfies the rule."""
        rule = make_rule(aggregation="avg", min_batches=1, window_minutes=10, threshold=400, operator="gt")
        records = [
            make_record(9, 0, 1, batchDuration=100),  # avg 100 → no
            make_record(9, 2, 2, batchDuration=500),  # avg (100 + 500) / 2 = 300 → no
            make_record(9, 11, 3),  # no batchDuration; window (09:01, 09:11] drops the 100 → avg 500 → fires here
        ]
        [decision] = DecisionEngine([rule]).evaluate_cluster("c", 4, records)
        assert decision.decision_ts == ts_raw(9, 11)

    def test_first_holding_rule_in_file_order_wins_when_several_hold(self):
        """README 6.2: when more than one rule's condition holds, the one listed first in the rules
        file is the matched rule, whatever the order they would fire in otherwise."""
        lower_target = make_rule(name="first", operator="gt", threshold=1, min_batches=1, window_minutes=5, target_workers=6)
        higher_target = make_rule(name="second", operator="gt", threshold=1, min_batches=1, window_minutes=5, target_workers=16)
        record = make_record(9, 0, 1, batchDuration=10)
        [d1] = DecisionEngine([lower_target, higher_target]).evaluate_cluster("c", 4, [record])
        [d2] = DecisionEngine([higher_target, lower_target]).evaluate_cluster("c", 4, [record])
        assert (d1.rule_name, d1.to_workers) == ("first", 6)
        assert (d2.rule_name, d2.to_workers) == ("second", 16)

    def test_no_rule_holding_moves_on_without_touching_state(self):
        """README 6.2: an evaluation point where no rule holds produces nothing and leaves the gates'
        state untouched, so a later point still sees the original worker count and no cooldown."""
        rule = make_rule(min_batches=1, window_minutes=1, threshold=100, operator="gt", cooldown_minutes=60)
        records = [
            make_record(9, 0, 1, batchDuration=1),  # holds? 1 > 100 → no → move on
            make_record(9, 30, 2, batchDuration=1),  # no again
            make_record(9, 31, 3, batchDuration=500),  # holds → 4 → 8 with no cooldown in the way
        ]
        decisions, events = evaluate_with_trace([rule], 4, records)
        assert list(events) == [ts_raw(9, 31)]
        assert [(d.decision_ts, d.from_workers, d.to_workers) for d in decisions] == [(ts_raw(9, 31), 4, 8)]


class TestParsedRecords:
    """Section 6 applied to records that went through the real parser, not hand-built dicts."""

    def test_log_line_prefix_timestamps_play_no_role(self):
        """README 6.1: ordering, windows and decision_ts all use the JSON `timestamp`; the log-line
        prefix timestamp is ignored even when it is on another day and in the opposite order."""
        rule = make_rule(min_batches=2, window_minutes=10, threshold=100, operator="gt")
        lines = [
            # prefix says 31 Dec 23:59, JSON says 09:04 — and this line comes first in the file
            progress_line("25/12/31 23:59:59", {"id": "q1", "batchId": 2, "timestamp": ts_raw(9, 4), "batchDuration": 500}),
            # prefix says 1 Jan 00:00, JSON says 09:00
            progress_line("26/01/01 00:00:00", {"id": "q1", "batchId": 1, "timestamp": ts_raw(9, 0), "batchDuration": 500}),
            # prefix 2 days earlier, JSON far outside any 10-minute window → never joins the others
            progress_line("25/11/01 09:02:00", {"id": "q1", "batchId": 3, "timestamp": ts_raw(11, 0), "batchDuration": 500}),
        ]
        records = SparkLogParser().parse_lines(lines)
        [decision] = DecisionEngine([rule]).evaluate_cluster("c", 4, records)
        # 2 records first available at the JSON time 09:04, sorted by JSON timestamp
        assert decision.decision_ts == ts_raw(9, 4)

    def test_unparseable_metric_does_not_count_toward_min_batches(self):
        """README 6.3 step 2: a record whose metric is present but not parseable does not carry it,
        so it neither counts toward min_batches nor enters the aggregate."""
        rule = make_rule(metric="source.numBytesOutstanding", aggregation="last", operator="gt", threshold=10, min_batches=2, window_minutes=10)

        def doc(minute: int, batch: int, outstanding) -> dict:
            return {
                "id": "q1",
                "batchId": batch,
                "timestamp": ts_raw(9, minute),
                "sources": [{"metrics": {"numBytesOutstanding": outstanding}}],
            }

        lines = [
            progress_line("25/11/03 09:00:00", doc(0, 1, "100")),
            progress_line("25/11/03 09:02:00", doc(2, 2, "n/a")),  # present but unparseable → not carried
            progress_line("25/11/03 09:04:00", doc(4, 3, "100")),
        ]
        records = SparkLogParser().parse_lines(lines)
        assert records[1]["metrics"].get("source.numBytesOutstanding") is None
        engine = DecisionEngine([rule])
        timestamps = [r["timestamp"] for r in records]
        assert engine.evaluate_rule(rule, records, timestamps, ts(9, 2)).records_with_metric == 1
        assert engine.condition_holds(rule, records, timestamps, ts(9, 2)) is False  # 1 < min_batches
        assert engine.condition_holds(rule, records, timestamps, ts(9, 4)) is True  # 2 parseable → holds


class TestEvaluateCluster:
    def test_decision_ts_is_verbatim_raw_timestamp(self):
        """README 6.1 / section 7: decision_ts is the record's original JSON timestamp string, byte for byte."""
        rule = make_rule(min_batches=1, window_minutes=5, threshold=1, operator="gt")
        record = make_record(9, 0, 1, batchDuration=10)
        [decision] = DecisionEngine([rule]).evaluate_cluster("c", 4, [record])
        assert decision.decision_ts == record["timestamp_raw"] == "2025-11-03T09:00:00.000Z"

    def test_cooldown_uses_matched_rules_cooldown_and_workers_chain(self):
        """README 6.4: each decision uses the matched rule's cooldown, worker counts chain across decisions,
        and a gated match does not reset last_decision_ts (down fires exactly 4 min after up's decision)."""
        # 1-minute windows so each evaluation point sees only its own record; that
        # keeps the focus on the gates rather than on window arithmetic.
        up = make_rule(name="up", metric="batchDuration", operator="gt", threshold=100, min_batches=1, window_minutes=1, target_workers=8, cooldown_minutes=10)
        down = make_rule(name="down", metric="numInputRows", operator="lt", threshold=100, min_batches=1, window_minutes=1, target_workers=2, cooldown_minutes=4)
        records = [
            make_record(9, 0, 1, batchDuration=500),  # up: 4 → 8
            make_record(9, 2, 2, numInputRows=1),  # down matches; 09:02 < 09:00 + 4 → blocked
            make_record(9, 4, 3, numInputRows=1),  # down: exactly 4 min → 8 → 2
            make_record(9, 6, 4, batchDuration=500),  # up matches; 09:06 < 09:04 + 10 → blocked
            make_record(9, 14, 5, batchDuration=500),  # up: exactly 10 min → 2 → 8
        ]
        decisions = DecisionEngine([up, down]).evaluate_cluster("c", 4, records)
        assert [(d.rule_name, d.from_workers, d.to_workers) for d in decisions] == [
            ("up", 4, 8),
            ("down", 8, 2),
            ("up", 2, 8),
        ]

    def test_duplicates_across_files_do_not_double_count(self):
        """README 6.1 / 6.2: three copies of one record are one evaluation point and cannot satisfy min_batches=3."""
        rule = make_rule(min_batches=3, window_minutes=10, threshold=1, operator="gt")
        record = make_record(9, 0, 1, batchDuration=10)
        assert DecisionEngine([rule]).evaluate_cluster("c", 4, [record, record, record]) == []

    def test_no_rules_no_decisions(self):
        """README 6.2: with no rule whose condition can hold, every evaluation point is skipped."""
        assert DecisionEngine([]).evaluate_cluster("c", 4, [make_record(9, 0, 1, batchDuration=10)]) == []


class TestValidateRules:
    def test_valid_rules_pass_through_unchanged(self):
        """A well-formed rules list comes back equal and in order (as a normalised copy)."""
        rules = [make_rule(name="a"), make_rule(name="b")]
        assert validate_rules(rules) == rules

    @pytest.mark.parametrize(
        "field,raw,expected",
        [
            ("threshold", "5e9", 5e9),
            ("threshold", "300000", 300000.0),
            ("window_minutes", "15", 15),
            ("min_batches", 3.0, 3),
            ("cooldown_minutes", "20.0", 20),
        ],
    )
    def test_lenient_numeric_spellings_are_coerced(self, field, raw, expected, caplog):
        """PyYAML reads `5e9` as a string and `3.0` as a float; both are unambiguous and accepted."""
        [rule] = validate_rules([make_rule(**{field: raw})])
        assert rule[field] == expected
        assert type(rule[field]) is type(expected)

    @pytest.mark.parametrize("field,raw", [("window_minutes", "1.5"), ("min_batches", 2.5), ("threshold", "five")])
    def test_non_coercible_numbers_still_rejected(self, field, raw):
        """A fractional int field or a non-numeric string is a real error, not something to guess at."""
        with pytest.raises(ValueError):
            validate_rules([make_rule(**{field: raw})])

    def test_input_rules_are_not_mutated(self):
        """Coercion works on a copy; the caller's dicts (and /rules output) keep the YAML values."""
        rule = make_rule(threshold="5e9")
        validate_rules([rule])
        assert rule["threshold"] == "5e9"

    @pytest.mark.parametrize(
        "bad",
        [
            dict(aggregation="median"),
            dict(operator="ge"),
            dict(threshold="high"),
            dict(window_minutes=0),
            dict(min_batches=0),
            dict(target_workers=-1),
            dict(name=None),
            dict(metric=""),
        ],
        ids=lambda d: next(iter(d)),
    )
    def test_invalid_field_is_logged_and_rejected(self, bad, caplog):
        """Each bad field is logged with the rule's position and makes the engine refuse to start."""
        rule = make_rule(**bad)
        if "metric" in bad:
            rule["metric"] = ""  # empty metric can never match anything
            with pytest.raises(ValueError):
                validate_rules([{**rule, "metric": None}])
            return
        with pytest.raises(ValueError):
            DecisionEngine([rule])
        assert any("rule #0" in r.message for r in caplog.records)

    def test_duplicate_names_rejected(self):
        """Two rules with the same name are ambiguous in the output and are rejected."""
        with pytest.raises(ValueError, match="duplicate"):
            validate_rules([make_rule(name="x"), make_rule(name="x")])

    def test_rules_must_be_a_list(self):
        """A rules document whose 'rules' key is not a list is rejected."""
        with pytest.raises(ValueError):
            validate_rules({"name": "x"})


class TestEvaluateStream:
    """Bounded-memory walk over chronologically ordered per-file batches."""

    rule = make_rule(name="slow", min_batches=3, window_minutes=10, threshold=100, operator="gt", target_workers=8, cooldown_minutes=0)

    def test_matches_full_buffer_evaluation(self):
        """Streaming file by file yields exactly the decisions of the all-at-once walk."""
        engine = DecisionEngine([self.rule, make_rule(name="down", metric="numInputRows", operator="lt", threshold=5, min_batches=2, window_minutes=6, target_workers=2, cooldown_minutes=4)])
        # three 1-hour files: slow batches, then idle, then slow again → up, down, up
        phases = [dict(batchDuration=500, numInputRows=99), dict(batchDuration=10, numInputRows=1), dict(batchDuration=500, numInputRows=99)]
        files = [[make_record(8 + h, m, h * 60 + m, **phases[h]) for m in range(0, 60, 2)] for h in range(3)]
        records = [r for f in files for r in f]
        full = engine.evaluate_cluster("c", 4, records)
        streamed = list(engine.evaluate_stream("c", 4, files))
        assert streamed == full
        assert [(d.from_workers, d.to_workers) for d in full] == [(4, 8), (8, 2), (2, 8)]

    def test_record_spilled_into_next_file_is_placed_correctly(self):
        """A 08:59 batch logged into the 09:00 file is evaluated in timestamp order, between the 08:xx records."""
        engine = DecisionEngine([self.rule])
        file_08 = [make_record(8, 50, 1, batchDuration=500), make_record(8, 55, 2, batchDuration=500)]
        file_09 = [make_record(8, 59, 3, batchDuration=500), make_record(9, 5, 4, batchDuration=500)]  # first is the spill
        decisions = list(engine.evaluate_stream("c", 4, [file_08, file_09]))
        # third record with the metric inside (08:49, 08:59] is the spilled one → decision at 08:59, not 09:05
        assert [d.decision_ts for d in decisions] == ["2025-11-03T08:59:00.000Z"]

    def test_records_are_held_back_until_next_file_confirms_them(self):
        """Nothing at or after a file's earliest timestamp is evaluated until the following file arrives."""
        engine = DecisionEngine([self.rule])
        seen_points = []
        engine._trace = lambda e: seen_points.append(e["evaluation_ts"])
        file_a = [make_record(9, m, m, batchDuration=500) for m in (0, 2, 4)]
        file_b = [make_record(9, m, m, batchDuration=500) for m in (6, 8)]

        def batches():
            yield file_a
            assert seen_points == []  # file_a alone: its records wait
            yield file_b
            assert seen_points == ["2025-11-03T09:04:00.000Z"]  # only the point < 09:06 (with 3 in window) fired

        list(engine.evaluate_stream("c", 4, batches()))

    def test_duplicates_across_files_are_dropped_and_counted(self):
        """The rotation-overlap copy in the next file is ignored; stats report it."""
        engine = DecisionEngine([self.rule])
        r = make_record(9, 0, 1, batchDuration=500)
        stats = StreamStats()
        list(engine.evaluate_stream("c", 4, [[r], [r, make_record(9, 2, 2, batchDuration=500)]], stats))
        assert (stats.records_seen, stats.unique_records) == (3, 2)

    def test_lookback_survives_file_boundary_and_memory_is_bounded(self):
        """Window records from the previous file are still available; peak held stays near two files."""
        engine = DecisionEngine([make_rule(min_batches=3, window_minutes=10, threshold=100, operator="gt")])
        files = [[make_record(9 + (m // 60), m % 60, m, batchDuration=500) for m in range(h * 60, h * 60 + 60, 2)] for h in range(6)]
        stats = StreamStats()
        decisions = list(engine.evaluate_stream("c", 4, files, stats))
        assert decisions[0].decision_ts == "2025-11-03T09:04:00.000Z"  # 3rd record overall
        assert stats.unique_records == 180
        assert stats.peak_held_records <= 2 * 30 + 6  # two files + look-back slack, not 180

    def test_empty_and_single_batch(self):
        """No files → no decisions; one file → same as evaluate_cluster."""
        engine = DecisionEngine([self.rule])
        assert list(engine.evaluate_stream("c", 4, [])) == []
        recs = [make_record(9, m, m, batchDuration=500) for m in (0, 2, 4)]
        assert list(engine.evaluate_stream("c", 4, [recs])) == engine.evaluate_cluster("c", 4, recs)
