"""Scaler orchestration with in-memory fakes for the three AWS clients."""
from __future__ import annotations

import gzip
import json
import time

import pytest

from autoscaler_service.aws_clients import BotoClientError, QueueMessage, S3ObjectNotFound
from autoscaler_service.config import Settings
from autoscaler_service.engine import DecisionEngine, Scaler, validate_manifest
from autoscaler_service.engine.scaler import ALL_PROCESSED_MESSAGE

from autoscaler_service.tests.conftest import make_rule

HDR = "25/11/03 09:00:00 INFO ProgressReporter: Streaming query made progress: "


def progress_line(hh: int, mm: int, batch_id: int, batch_duration: int = 500_000) -> str:
    doc = {
        "id": "q1",
        "batchId": batch_id,
        "timestamp": f"2025-11-03T{hh:02d}:{mm:02d}:00.000Z",
        "batchDuration": batch_duration,
        "numInputRows": 10_000,
    }
    return HDR + json.dumps(doc)


class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    def put_gz(self, key: str, lines: list[str]) -> None:
        self.objects[key] = gzip.compress("\n".join(lines).encode())

    def ensure_object_exists(self, key, bucket=None):
        if key not in self.objects:
            raise S3ObjectNotFound(f"{key} missing", code="NoSuchKey")
        return len(self.objects[key])

    def iter_log_lines(self, key, bucket=None):
        if key not in self.objects:
            raise S3ObjectNotFound(f"{key} missing", code="NoSuchKey")
        self.reads = getattr(self, "reads", []) + [key]
        yield from gzip.decompress(self.objects[key]).decode().splitlines()


class FakeSqs:
    def __init__(self):
        self.acked: list[str] = []
        self.pending: list[QueueMessage] = []
        self.extended: list[str] = []

    def extend_visibility(self, message, seconds=None):
        self.extended.append(message.receipt_handle)

    def read_msg_from_queue(self, *a, **k):
        batch, self.pending = self.pending, []
        return batch

    def ack_message_processed(self, message):
        self.acked.append(message.receipt_handle)

    def reset_queue_url(self):
        pass


class FakeDynamo:
    def __init__(self, fail_after: int | None = None):
        self.items = {}
        self.fail_after = fail_after
        self.writes = 0

    def put_decisions(self, decisions):
        for d in decisions:
            if self.fail_after is not None and self.writes >= self.fail_after:
                raise BotoClientError("boom", code="ProvisionedThroughputExceededException")
            self.items[(d.cluster_id, d.decision_ts)] = d
            self.writes += 1
        return len(decisions)


def message(cluster_id: str, key: str, bucket: str = "b") -> QueueMessage:
    body = json.dumps({"bucket": bucket, "key": key, "cluster_id": cluster_id})
    return QueueMessage(message_id=key, receipt_handle="rh-" + key, body=body)


@pytest.fixture
def world():
    manifest = [
        {"cluster_id": "alpha", "initial_workers": 4, "num_log_files": 2},
        {"cluster_id": "beta", "initial_workers": 8, "num_log_files": 1},
    ]
    rule = make_rule(name="slow", min_batches=3, window_minutes=15, threshold=300_000, target_workers=8)
    s3, sqs, ddb = FakeS3(), FakeSqs(), FakeDynamo()
    # alpha's two files: 09:00-09:06 in file A, 09:08-09:14 in file B, overlapping batch 4.
    s3.put_gz("logs/alpha/a.gz", [progress_line(9, m, m // 2) for m in range(0, 8, 2)] + [progress_line(9, 8, 4)])
    s3.put_gz("logs/alpha/b.gz", [progress_line(9, m, m // 2) for m in range(8, 16, 2)])
    # beta: already at 8 workers, rule target 8 → idempotence gate, no decisions.
    s3.put_gz("logs/beta/a.gz", [progress_line(10, m, m // 2) for m in range(0, 10, 2)])
    settings = Settings(aws_endpoint_url=None, sqs_wait_time_seconds=0, startup_retry_seconds=0)
    scaler = Scaler(settings, sqs=sqs, s3=s3, dynamodb=ddb, manifest=manifest, engine=DecisionEngine([rule]))
    return scaler, s3, sqs, ddb


class TestBuffering:
    def test_out_of_order_files_are_buffered_until_cluster_complete(self, world, capsys):
        """README 6.1: the record set is all of a cluster's files, so a later file arriving first is held
        and evaluation runs only when the second file lands."""
        scaler, s3, sqs, ddb = world
        assert scaler.handle_message(message("alpha", "logs/alpha/b.gz")) is True  # later file first
        assert ddb.items == {}
        assert scaler.status()["clusters_processed"] == 0

        assert scaler.handle_message(message("alpha", "logs/alpha/a.gz")) is True
        # 3 records with avg > 300000 first available at 09:04 (09:00, 09:02, 09:04)
        assert list(ddb.items) == [("alpha", "2025-11-03T09:04:00.000Z")]
        decision = ddb.items[("alpha", "2025-11-03T09:04:00.000Z")]
        assert (decision.from_workers, decision.to_workers, decision.rule_name) == (4, 8, "slow")
        assert scaler.status()["clusters_processed"] == 1
        assert not scaler.all_processed
        assert ALL_PROCESSED_MESSAGE not in capsys.readouterr().out

    def test_overlapping_batch_is_counted_once(self, world):
        """README 6.1: a batch present in two files (rotation overlap) is a single record in the buffer."""
        scaler, s3, sqs, ddb = world
        scaler.handle_message(message("alpha", "logs/alpha/a.gz"))
        scaler.handle_message(message("alpha", "logs/alpha/b.gz"))
        assert scaler.status()["clusters"][0]["unique_records"] == 8  # 09:00..09:14 every 2 min

    def test_duplicate_delivery_of_same_file_does_not_complete_cluster(self, world):
        """Redelivery of the same file is acked but does not count as a new file."""
        scaler, s3, sqs, ddb = world
        scaler.handle_message(message("alpha", "logs/alpha/a.gz"))
        scaler.handle_message(message("alpha", "logs/alpha/a.gz"))
        assert scaler.status()["clusters_processed"] == 0
        assert len(sqs.acked) == 2  # both copies acked

    def test_all_clusters_processed_prints_sentinel_once(self, world, capsys):
        """ALL CLUSTERS PROCESSED is printed exactly once when the last cluster completes;
        beta starts at the rule's target so README 6.4 idempotence yields no decision for it."""
        scaler, s3, sqs, ddb = world
        for cid, key in [("alpha", "logs/alpha/a.gz"), ("beta", "logs/beta/a.gz"), ("alpha", "logs/alpha/b.gz")]:
            scaler.handle_message(message(cid, key))
        assert scaler.all_processed
        assert scaler.wait_all_processed(timeout=0)
        assert capsys.readouterr().out.count(ALL_PROCESSED_MESSAGE) == 1
        assert ("beta", "2025-11-03T10:00:00.000Z") not in ddb.items  # idempotence gate

    def test_files_are_not_downloaded_until_the_cluster_is_complete(self, world):
        """Arrival only registers the key (after a HEAD); parsing happens once, in order, at completion."""
        scaler, s3, sqs, ddb = world
        scaler.handle_message(message("alpha", "logs/alpha/b.gz"))
        assert getattr(s3, "reads", []) == []  # nothing streamed yet
        assert set(scaler._buffers["alpha"].files) == {"logs/alpha/b.gz"}
        scaler.handle_message(message("alpha", "logs/alpha/a.gz"))
        # fallback ordering (keys carry no hour): first-record peek of each file, then the ordered walk
        assert s3.reads[-2:] == ["logs/alpha/a.gz", "logs/alpha/b.gz"]
        assert scaler.status()["clusters"][0]["unique_records"] == 8

    def test_late_file_after_evaluation_is_acked_and_ignored(self, world):
        """A file arriving after its cluster was evaluated is dropped without re-evaluating."""
        scaler, s3, sqs, ddb = world
        scaler.handle_message(message("beta", "logs/beta/a.gz"))
        assert scaler.status()["clusters"][1]["evaluated"] is True
        assert scaler.handle_message(message("beta", "logs/beta/a.gz")) is True
        assert ddb.writes == 0


class TestPoisonAndTransientMessages:
    def test_invalid_body_is_acked_and_dropped(self, world):
        """A message whose body is not a valid notification is deleted, not retried forever."""
        scaler, s3, sqs, ddb = world
        bad = QueueMessage(message_id="x", receipt_handle="rh-x", body="not json")
        assert scaler.handle_message(bad) is True
        assert sqs.acked == ["rh-x"]

    def test_unknown_cluster_is_acked_and_dropped(self, world):
        """A message for a cluster missing from the manifest is deleted and ignored."""
        scaler, s3, sqs, ddb = world
        s3.put_gz("logs/ghost/a.gz", [progress_line(9, 0, 1)])
        assert scaler.handle_message(message("ghost", "logs/ghost/a.gz")) is True
        assert scaler.status()["clusters_processed"] == 0

    def test_missing_s3_object_is_acked_and_dropped(self, world):
        """A message pointing at a non-existent S3 key is deleted and the buffer stays untouched."""
        scaler, s3, sqs, ddb = world
        assert scaler.handle_message(message("alpha", "logs/alpha/nope.gz")) is True
        assert scaler.status()["clusters"][0]["files_received"] == 0

    def test_dynamodb_failure_leaves_message_for_redelivery_then_succeeds(self, world):
        """A failed write leaves the message unacked; the redelivered message completes the cluster."""
        scaler, s3, sqs, ddb = world
        ddb.fail_after = 0
        scaler.handle_message(message("beta", "logs/beta/a.gz"))  # beta has no decisions → ok
        scaler.handle_message(message("alpha", "logs/alpha/a.gz"))
        assert scaler.handle_message(message("alpha", "logs/alpha/b.gz")) is False  # write failed → not acked
        assert scaler.status()["clusters"][0]["evaluated"] is False
        assert not scaler.all_processed

        ddb.fail_after = None  # "AWS recovered"; SQS redelivers the same message
        assert scaler.handle_message(message("alpha", "logs/alpha/b.gz")) is True
        assert scaler.status()["clusters"][0]["evaluated"] is True
        assert len(ddb.items) == 1
        assert scaler.all_processed


class TestWorkerThreads:
    def test_start_and_stop_drain_queue(self, world):
        """Worker threads consume every pending message and stop cleanly."""
        scaler, s3, sqs, ddb = world
        sqs.pending = [
            message("alpha", "logs/alpha/b.gz"),
            message("beta", "logs/beta/a.gz"),
            message("alpha", "logs/alpha/a.gz"),
        ]
        scaler.start()
        try:
            assert scaler.wait_all_processed(timeout=5)
        finally:
            scaler.stop(timeout=5)
        assert scaler.status()["workers_alive"] == 0
        assert len(sqs.acked) == 3
        assert len(ddb.items) == 1

    def test_receive_errors_do_not_kill_worker(self, world):
        """An SQS receive error is logged and retried; the worker keeps running."""
        scaler, s3, sqs, ddb = world
        calls = {"n": 0}

        def flaky_receive(*a, **k):
            calls["n"] += 1
            if calls["n"] == 1:
                raise BotoClientError("queue not there", code="AWS.SimpleQueueService.NonExistentQueue")
            return [message("beta", "logs/beta/a.gz")] if calls["n"] == 2 else []

        sqs.read_msg_from_queue = flaky_receive
        scaler.start()
        try:
            import time

            deadline = time.time() + 5
            while scaler.status()["clusters_processed"] < 1 and time.time() < deadline:
                time.sleep(0.05)
        finally:
            scaler.stop(timeout=5)
        assert scaler.status()["clusters_processed"] == 1


    def test_unexpected_receive_error_does_not_kill_worker(self, world):
        """A non-AWS exception while polling (a bug, a bad response shape) is logged and the worker survives."""
        scaler, s3, sqs, ddb = world
        calls = {"n": 0}

        def broken_then_fine(*a, **k):
            calls["n"] += 1
            if calls["n"] == 1:
                raise KeyError("Messages")  # e.g. an unexpected response shape
            return [message("beta", "logs/beta/a.gz")] if calls["n"] == 2 else []

        sqs.read_msg_from_queue = broken_then_fine
        scaler.start()
        try:
            import time

            deadline = time.time() + 5
            while scaler.status()["clusters_processed"] < 1 and time.time() < deadline:
                time.sleep(0.05)
            assert scaler.workers_alive() == 1
        finally:
            scaler.stop(timeout=5)
        assert scaler.status()["clusters_processed"] == 1


class TestDebugLogging:
    def test_worker_loop_records_every_received_message(self, world):
        """In debug mode each message read from SQS is passed to the debug logger before handling."""
        scaler, s3, sqs, ddb = world
        seen = []

        class Recorder:
            def log_sqs_message(self, message):
                seen.append(message.message_id)

        scaler._debug_logger = Recorder()
        sqs.pending = [message("alpha", "logs/alpha/a.gz"), message("beta", "logs/beta/a.gz")]
        scaler.start()
        try:
            import time

            deadline = time.time() + 5
            while len(sqs.acked) < 2 and time.time() < deadline:
                time.sleep(0.05)
        finally:
            scaler.stop(timeout=5)
        assert seen == ["logs/alpha/a.gz", "logs/beta/a.gz"]


class TestValidateManifest:
    def test_valid_manifest_passes_through(self):
        """A well-formed clusters list is returned unchanged."""
        clusters = [{"cluster_id": "a", "initial_workers": 0, "num_log_files": 1}]
        assert validate_manifest(clusters) is clusters

    @pytest.mark.parametrize(
        "entry",
        [
            {"initial_workers": 4, "num_log_files": 1},
            {"cluster_id": "", "initial_workers": 4, "num_log_files": 1},
            {"cluster_id": "a", "initial_workers": -1, "num_log_files": 1},
            {"cluster_id": "a", "initial_workers": 4, "num_log_files": 0},
            {"cluster_id": "a", "initial_workers": "4", "num_log_files": 1},
            "not a mapping",
        ],
    )
    def test_invalid_entry_is_logged_and_rejected(self, entry, caplog):
        """A bad manifest entry is logged and makes Scaler construction fail."""
        with pytest.raises(ValueError):
            validate_manifest([entry])
        assert any("invalid manifest" in r.message for r in caplog.records)

    def test_duplicate_cluster_ids_rejected(self):
        """Two entries for the same cluster would make the file count ambiguous."""
        entry = {"cluster_id": "a", "initial_workers": 4, "num_log_files": 1}
        with pytest.raises(ValueError, match="duplicate"):
            validate_manifest([entry, dict(entry)])


import threading


class TestLockReleasedDuringWrites:
    def test_other_clusters_progress_while_one_is_being_written(self, world):
        """A slow DynamoDB write for beta must not block alpha's file from being merged."""
        scaler, s3, sqs, ddb = world
        release = threading.Event()
        started = threading.Event()
        real_put = ddb.put_decisions

        def slow_put(decisions):
            started.set()
            assert release.wait(5), "test released too late"
            return real_put(decisions)

        ddb.put_decisions = slow_put
        beta_thread = threading.Thread(target=scaler.handle_message, args=(message("beta", "logs/beta/a.gz"),))
        beta_thread.start()
        assert started.wait(5)  # beta is now inside its write, lock must be free
        t0 = time.time()
        assert scaler.handle_message(message("alpha", "logs/alpha/a.gz")) is True
        assert time.time() - t0 < 1.0  # did not wait for beta's write
        assert scaler.status()["clusters"][0]["files_received"] == 1
        assert scaler.status()["clusters"][1]["evaluating"] is True
        release.set()
        beta_thread.join(5)
        assert scaler.status()["clusters"][1]["evaluated"] is True

    def test_duplicate_during_evaluation_is_acked_and_ignored(self, world):
        """A second copy of beta's file arriving mid-write backs off; the owner finishes alone."""
        scaler, s3, sqs, ddb = world
        release, started = threading.Event(), threading.Event()
        real_put = ddb.put_decisions

        def slow_put(decisions):
            started.set()
            release.wait(5)
            return real_put(decisions)

        ddb.put_decisions = slow_put
        threading.Thread(target=scaler.handle_message, args=(message("beta", "logs/beta/a.gz"),)).start()
        assert started.wait(5)
        assert scaler.handle_message(message("beta", "logs/beta/a.gz")) is True  # acked, ignored
        release.set()
        time.sleep(0.2)
        assert scaler.status()["clusters"][1]["evaluated"] is True
        assert scaler.status()["clusters"][1]["decisions"] == 0

    def test_write_failure_resets_evaluating_so_redelivery_retries(self, world):
        """If the write fails the cluster is no longer 'evaluating'; the redelivered message evaluates it again."""
        scaler, s3, sqs, ddb = world
        scaler.handle_message(message("alpha", "logs/alpha/a.gz"))
        ddb.fail_after = 0
        assert scaler.handle_message(message("alpha", "logs/alpha/b.gz")) is False
        alpha = scaler.status()["clusters"][0]
        assert (alpha["evaluating"], alpha["evaluated"]) == (False, False)
        ddb.fail_after = None
        assert scaler.handle_message(message("alpha", "logs/alpha/b.gz")) is True
        assert scaler.status()["clusters"][0]["evaluated"] is True
        assert len(ddb.items) == 1


class TestVisibilityHeartbeat:
    def test_long_handling_extends_visibility_periodically(self, world):
        """While a message is being processed its visibility is extended every heartbeat interval."""
        scaler, s3, sqs, ddb = world
        scaler._settings = Settings(aws_endpoint_url=None, sqs_wait_time_seconds=0, startup_retry_seconds=0,
                                    sqs_visibility_heartbeat_seconds=1, sqs_visibility_extension_seconds=2)
        real_put = ddb.put_decisions

        def slow_put(decisions):
            time.sleep(2.3)
            return real_put(decisions)

        ddb.put_decisions = slow_put
        assert scaler.handle_message(message("beta", "logs/beta/a.gz")) is True
        assert sqs.extended.count("rh-logs/beta/a.gz") >= 2
        assert not any(th.name.startswith("visibility-") and th.is_alive() for th in threading.enumerate())

    def test_fast_handling_never_calls_extend(self, world):
        """Sub-second processing finishes before the first heartbeat, so no extension is sent."""
        scaler, s3, sqs, ddb = world
        scaler.handle_message(message("beta", "logs/beta/a.gz"))
        assert sqs.extended == []

    def test_extend_failure_is_not_fatal(self, world, caplog):
        """A failing ChangeMessageVisibility is logged and processing still completes and acks."""
        scaler, s3, sqs, ddb = world
        scaler._settings = Settings(aws_endpoint_url=None, sqs_wait_time_seconds=0, startup_retry_seconds=0,
                                    sqs_visibility_heartbeat_seconds=1, sqs_visibility_extension_seconds=2)

        def boom(message, seconds=None):
            raise BotoClientError("ReceiptHandleIsInvalid", code="ReceiptHandleIsInvalid")

        sqs.extend_visibility = boom
        real_put = ddb.put_decisions
        ddb.put_decisions = lambda d: (time.sleep(1.3), real_put(d))[1]
        assert scaler.handle_message(message("beta", "logs/beta/a.gz")) is True
        assert any("could not extend visibility" in r.message for r in caplog.records)


from datetime import datetime, timezone

from autoscaler_service.engine.scaler import hour_from_key, order_log_keys


class TestOrderLogKeys:
    def test_orders_by_hour_in_file_name(self):
        """README 4.4 naming: the hour in the key decides the order, regardless of arrival order."""
        keys = ["logs/c/log4j-2025-11-03-10.log.gz", "logs/c/log4j-2025-11-03-08.log.gz", "logs/c/log4j-2025-11-03-09.log.gz"]
        assert order_log_keys(keys, lambda k: pytest.fail("no peek needed")) == sorted(keys)
        assert hour_from_key(keys[0]) == datetime(2025, 11, 3, 10, tzinfo=timezone.utc)

    def test_falls_back_to_first_record_timestamp(self):
        """When a key does not encode an hour, files are ordered by their first record; unknown ones go last."""
        stamps = {"b.gz": datetime(2025, 11, 3, 9, tzinfo=timezone.utc), "a.gz": datetime(2025, 11, 3, 8, tzinfo=timezone.utc), "empty.gz": None}
        assert order_log_keys(["b.gz", "empty.gz", "a.gz"], stamps.__getitem__) == ["a.gz", "b.gz", "empty.gz"]

    def test_day_boundary(self):
        """23:00 on the 3rd sorts before 00:00 on the 4th even though '00' < '23' as a string."""
        keys = ["log4j-2025-11-04-00.log.gz", "log4j-2025-11-03-23.log.gz"]
        assert order_log_keys(keys, lambda k: None) == ["log4j-2025-11-03-23.log.gz", "log4j-2025-11-04-00.log.gz"]
