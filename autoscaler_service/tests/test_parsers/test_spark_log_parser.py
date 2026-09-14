import gzip
import json
from datetime import datetime, timezone

import pytest

from autoscaler_service.parsers import ParseStats, SparkLogParser, parse_timestamp

HDR = "25/11/03 09:14:00 INFO "


def progress_doc(**overrides):
    doc = {
        "id": "a91c",
        "runId": "77b0",
        "name": "ingest",
        "timestamp": "2025-11-03T09:14:00.000Z",
        "batchId": 247,
        "batchDuration": 78450,
        "numInputRows": 152340,
        "inputRowsPerSecond": 1269.5,
        "processedRowsPerSecond": 1941.9,
        "durationMs": {"addBatch": 71234, "triggerExecution": 78450},
        "stateOperators": [],
        "sources": [
            {
                "description": "CloudFilesSource[s3://x]",
                "startOffset": {"seqNum": 1},
                "endOffset": {"seqNum": 2},
                "numInputRows": 152340,
                "metrics": {
                    "numBytesOutstanding": "5388009472",
                    "numFilesOutstanding": "1287",
                    "approximateQueueSize": "1287",
                },
            }
        ],
        "sink": {"description": "DeltaSink[x]", "numOutputRows": 152340},
    }
    doc.update(overrides)
    return doc


def single_line(doc) -> str:
    return HDR + "ProgressReporter: Streaming query made progress: " + json.dumps(doc)


def multi_line(doc) -> str:
    return HDR + "MicroBatchExecution: Streaming query made progress: " + json.dumps(doc, indent=2, separators=(",", " : "))


@pytest.fixture
def parser():
    return SparkLogParser()


class TestEntryGrouping:
    def test_continuation_lines_belong_to_previous_entry(self, parser):
        """Stack-trace lines without a timestamp header are attached to the preceding entry."""
        lines = [
            HDR + "A: first",
            "\tat some.StackFrame(File.java:1)",
            "Caused by: boom",
            HDR + "B: second",
        ]
        entries = list(parser.iter_entries(lines))
        assert len(entries) == 2
        assert entries[0].count("\n") == 2
        assert entries[1] == HDR + "B: second"

    def test_lines_before_first_header_form_their_own_entry(self, parser):
        """Text before the first header (rotation tail) becomes a separate, harmless entry."""
        lines = ['  "tail": "of a rotated entry"}', HDR + "A: real"]
        entries = list(parser.iter_entries(lines))
        assert entries == ['  "tail": "of a rotated entry"}', HDR + "A: real"]

    def test_empty_input(self, parser):
        """No lines yields no entries."""
        assert list(parser.iter_entries([])) == []


class TestRecordExtraction:
    def test_single_line_format(self, parser):
        """README 6.1: the record timestamp comes from the JSON `timestamp` field (UTC), kept verbatim as timestamp_raw."""
        [record] = parser.parse_text(single_line(progress_doc()))
        assert record["id"] == "a91c"
        assert record["batchId"] == 247
        assert record["timestamp"] == datetime(2025, 11, 3, 9, 14, tzinfo=timezone.utc)
        assert record["timestamp_raw"] == "2025-11-03T09:14:00.000Z"
        assert set(record) == {"id", "batchId", "timestamp", "timestamp_raw", "metrics"}

    def test_multi_line_format_is_identical(self, parser):
        """The pretty-printed MicroBatchExecution format produces the exact same record."""
        [a] = parser.parse_text(single_line(progress_doc()))
        [b] = parser.parse_text(multi_line(progress_doc()))
        assert a == b

    def test_top_level_and_source_metrics(self, parser):
        """Top-level numbers keep their name, source metrics are 'source.'-prefixed and parsed from strings."""
        [record] = parser.parse_text(single_line(progress_doc()))
        assert record["metrics"].get("batchDuration") == 78450
        assert record["metrics"].get("numInputRows") == 152340
        assert record["metrics"].get("inputRowsPerSecond") == 1269.5
        assert record["metrics"].get("processedRowsPerSecond") == 1941.9
        # source metrics are strings in the log and numbers here
        assert record["metrics"].get("source.numBytesOutstanding") == 5388009472.0
        assert record["metrics"].get("source.numFilesOutstanding") == 1287.0
        # identity fields and nested objects are not metrics
        for name in ("id", "runId", "name", "timestamp", "batchId", "durationMs", "sources", "sink"):
            assert name not in record["metrics"]

    def test_missing_batch_duration_is_still_a_valid_record(self, parser):
        """README 6.2: a record without batchDuration is kept (it is still an evaluation point); only that metric is absent."""
        doc = progress_doc()
        del doc["batchDuration"]
        [record] = parser.parse_text(single_line(doc))
        assert record["metrics"].get("batchDuration") is None
        assert record["metrics"].get("numInputRows") == 152340

    def test_unparseable_source_metric_is_dropped_not_fatal(self, parser):
        """README 6.3 step 2: a non-numeric (unparseable) source metric is omitted while its siblings are kept."""
        doc = progress_doc()
        doc["sources"][0]["metrics"]["numBytesOutstanding"] = "n/a"
        [record] = parser.parse_text(single_line(doc))
        assert record["metrics"].get("source.numBytesOutstanding") is None
        assert record["metrics"].get("source.numFilesOutstanding") == 1287.0

    def test_no_sources_means_no_source_metrics(self, parser):
        """An empty sources array produces no source.* metrics."""
        [record] = parser.parse_text(single_line(progress_doc(sources=[])))
        assert not any(k.startswith("source.") for k in record["metrics"])

    def test_boolean_is_not_a_metric(self, parser):
        """JSON booleans are not treated as numbers."""
        [record] = parser.parse_text(single_line(progress_doc(isTriggerActive=True)))
        assert record["metrics"].get("isTriggerActive") is None

    def test_trailing_text_after_json_is_tolerated(self, parser):
        """Extra continuation text after the JSON document does not invalidate the record."""
        text = single_line(progress_doc()) + "\nsome trailing line that is not a header"
        assert len(parser.parse_text(text)) == 1


class TestSkipping:
    def test_lookalike_marker_is_ignored(self, parser):
        """'Streaming query has been idle' does not match the exact progress marker."""
        text = HDR + "MicroBatchExecution: Streaming query has been idle and waiting for new data more than 24140 ms."
        stats = ParseStats()
        assert parser.parse_text(text, stats) == []
        assert stats.progress_entries == 0

    def test_truncated_json_is_skipped(self, parser):
        """A record cut off mid-write is counted as malformed and skipped."""
        text = single_line(progress_doc())[:-40]  # rotated mid-entry
        stats = ParseStats()
        assert parser.parse_text(text, stats) == []
        assert stats.malformed_json == 1

    @pytest.mark.parametrize("missing", ["id", "batchId", "timestamp"])
    def test_missing_required_field_is_skipped(self, parser, missing):
        """A record lacking id, batchId or timestamp is skipped as incomplete."""
        doc = progress_doc()
        del doc[missing]
        stats = ParseStats()
        assert parser.parse_text(single_line(doc), stats) == []
        assert stats.missing_required == 1

    def test_bad_timestamp_is_skipped(self, parser):
        """An unparseable timestamp makes the record incomplete."""
        stats = ParseStats()
        assert parser.parse_text(single_line(progress_doc(timestamp="yesterday")), stats) == []
        assert stats.missing_required == 1

    def test_payload_that_is_not_an_object_is_skipped(self, parser):
        """A JSON array after the marker is treated as malformed."""
        text = HDR + "X: Streaming query made progress: [1, 2, 3]"
        stats = ParseStats()
        assert parser.parse_text(text, stats) == []
        assert stats.malformed_json == 1

    def test_one_bad_record_does_not_hide_its_neighbours(self, parser):
        """Records before and after a malformed one are still parsed."""
        good1 = single_line(progress_doc(batchId=1))
        bad = single_line(progress_doc(batchId=2))[:-10]
        good2 = multi_line(progress_doc(batchId=3))
        records = parser.parse_text("\n".join([good1, bad, good2]))
        assert [r["batchId"] for r in records] == [1, 3]

    def test_duplicates_are_preserved_for_the_caller_to_dedup(self, parser):
        """README 6.1: the parser returns both copies of a duplicated batch; dedup by (id, batchId) belongs to the engine."""
        text = "\n".join([single_line(progress_doc()), multi_line(progress_doc())])
        assert len(parser.parse_text(text)) == 2


class TestTimestamp:
    def test_z_suffix(self):
        """A trailing Z is understood as UTC."""
        assert parse_timestamp("2025-11-03T09:14:00.000Z") == datetime(2025, 11, 3, 9, 14, tzinfo=timezone.utc)

    def test_offset_is_normalised_to_utc(self):
        """A non-zero offset is converted to UTC."""
        assert parse_timestamp("2025-11-03T11:14:00+02:00") == datetime(2025, 11, 3, 9, 14, tzinfo=timezone.utc)

    def test_naive_is_assumed_utc(self):
        """A timestamp without zone information is treated as UTC."""
        assert parse_timestamp("2025-11-03T09:14:00") == datetime(2025, 11, 3, 9, 14, tzinfo=timezone.utc)

    @pytest.mark.parametrize("bad", ["", "   ", None, 12345, "not-a-date"])
    def test_invalid(self, bad):
        """Empty, None, numeric and garbage timestamps raise ValueError."""
        with pytest.raises(ValueError):
            parse_timestamp(bad)


class TestBatchIdCoercion:
    @pytest.mark.parametrize("raw,expected", [(5, 5), (5.0, 5), ("5", 5)])
    def test_accepted(self, parser, raw, expected):
        """Integer, integral float and digit-string batchIds are all accepted as int."""
        [record] = parser.parse_text(single_line(progress_doc(batchId=raw)))
        assert record["batchId"] == expected

    @pytest.mark.parametrize("raw", [5.5, True, "five", None])
    def test_rejected(self, parser, raw):
        """Fractional, boolean, textual and missing batchIds invalidate the record."""
        assert parser.parse_text(single_line(progress_doc(batchId=raw))) == []


class TestRealSampleData:
    def test_borealis_file_with_known_malformed_record(self, parser, sample_log_dir):
        """The real borealis 08:00 file yields 14 of 15 records, two without batchDuration."""
        path = sample_log_dir / "cluster-borealis" / "log4j-2025-11-03-08.log.gz"
        if not path.exists():
            pytest.skip("sample file missing")
        stats = ParseStats()
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as fh:
            records = parser.parse_lines((line.rstrip("\n") for line in fh), stats)
        assert stats.progress_entries == 15
        assert stats.malformed_json == 1
        assert len(records) == 14
        assert sum(1 for r in records if "batchDuration" not in r["metrics"]) == 2
        assert all(r["timestamp"].tzinfo is timezone.utc for r in records)
