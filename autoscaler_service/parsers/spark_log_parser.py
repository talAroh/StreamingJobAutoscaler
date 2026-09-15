"""Extract streaming progress records from Spark driver logs.

The log format (README 4.4):

* An entry starts with a header line ``YY/MM/DD HH:MM:SS LEVEL Logger: message``.
* Every following line that is *not* a header belongs to the same entry (stack
  traces, pretty-printed JSON, config dumps).
* Only entries whose text contains the exact marker ``Streaming query made progress:``
  are progress records; the JSON document follows the marker, possibly spanning lines.

The parser is deliberately lenient about *content* (a bad record is skipped, never
fatal) and strict about *identity* (``id``, ``batchId``, ``timestamp`` must be present
and well-formed, otherwise the record cannot be deduplicated or ordered).
Deduplication itself is not done here: the same batch legitimately appears in two
files, so the per-cluster buffer in the scaler owns that concern.
"""
from __future__ import annotations

import json
import logging
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Iterator

logger = logging.getLogger(__name__)

#: A parsed progress record is a plain dict with exactly these keys:
#:   id            str       streaming query id
#:   batchId       int
#:   timestamp     datetime  UTC-aware, parsed from the JSON ``timestamp``
#:   timestamp_raw str       the JSON ``timestamp`` verbatim (becomes ``decision_ts``)
#:   metrics       dict      flat ``name -> float``; ``source.<name>`` for source metrics
ProgressRecord = dict[str, Any]


def dedup_key(record: ProgressRecord) -> tuple[str, int]:
    """Identity of a batch report: the same batch may be logged several times."""
    return (record["id"], record["batchId"])


def sort_key(record: ProgressRecord) -> tuple[datetime, int]:
    """Ordering of README 6.1: ascending ``(timestamp, batchId)``."""
    return (record["timestamp"], record["batchId"])

#: ``25/11/03 09:14:00 `` — the only thing that starts a new log entry.
HEADER_RE = re.compile(r"^\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} ")

PROGRESS_MARKER = "Streaming query made progress:"

#: Top-level JSON fields that identify a record rather than measure it.
IDENTITY_FIELDS = frozenset({"id", "runId", "name", "timestamp", "batchId"})

SOURCE_METRIC_PREFIX = "source."


@dataclass
class ParseStats:
    """Counters for one parse run; surfaced in logs so bad input is visible."""

    entries: int = 0  # log entries seen (header-delimited)
    progress_entries: int = 0  # entries carrying the marker
    records: int = 0  # valid ProgressRecords produced
    malformed_json: int = 0
    missing_required: int = 0
    skipped_reasons: list[str] = field(default_factory=list)

    def note_skip(self, reason: str) -> None:
        # Keep a bounded sample; the counts above are the real signal.
        if len(self.skipped_reasons) < 20:
            self.skipped_reasons.append(reason)


def parse_timestamp(value: Any) -> datetime:
    """Parse the record's ISO-8601 ``timestamp`` (e.g. ``2025-11-03T09:14:00.000Z``).

    ``datetime.fromisoformat`` on Python 3.10 rejects a trailing ``Z``, so it is
    rewritten to ``+00:00``. Naive values are assumed UTC; the result is always UTC.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"timestamp must be a non-empty string, got {value!r}")
    text = value.strip()
    if text[-1] in ("Z", "z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _to_number(value: Any) -> float | None:
    """Coerce a JSON scalar to a finite float, or ``None`` if it isn't one.

    Source metrics arrive as strings (``"5388009472"``); top-level metrics as numbers.
    ``bool`` is excluded explicitly because it is an ``int`` subclass in Python.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value.strip())
        except ValueError:
            return None
    else:
        return None
    return number if math.isfinite(number) else None


def _to_batch_id(value: Any) -> int | None:
    """``batchId`` should be an integer; tolerate an integral float or digit string."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


class SparkLogParser:
    """Stateless parser: feed it lines, get progress-record dicts back."""

    # -- entry grouping ---------------------------------------------------------

    @staticmethod
    def iter_entries(lines: Iterable[str]) -> Iterator[str]:
        """Group raw lines into log entries (header line + its continuation lines).

        Lines before the first header (a file rotated mid-entry) form an entry of
        their own; it can never contain the marker *and* a complete document, so it
        is harmlessly ignored downstream.
        """
        current: list[str] = []
        for line in lines:
            if HEADER_RE.match(line):
                if current:
                    yield "\n".join(current)
                current = [line]
            else:
                current.append(line)
        if current:
            yield "\n".join(current)

    # -- record extraction ------------------------------------------------------

    @staticmethod
    def extract_metrics(document: dict[str, Any]) -> dict[str, float]:
        """Flatten the metrics a rule may reference (README section 5).

        * every numeric top-level field except the identity fields keeps its name;
        * ``sources[0].metrics.<name>`` becomes ``source.<name>``, string → number.
        Anything that fails to coerce is left out, which is exactly the "record
        lacks this metric" case in 6.3 step 2.
        """
        metrics: dict[str, float] = {}
        for key, value in document.items():
            if key in IDENTITY_FIELDS:
                continue
            number = _to_number(value)
            if number is not None:
                metrics[key] = number

        sources = document.get("sources")
        if isinstance(sources, list) and sources and isinstance(sources[0], dict):
            source_metrics = sources[0].get("metrics")
            if isinstance(source_metrics, dict):
                for key, value in source_metrics.items():
                    number = _to_number(value)
                    if number is not None:
                        metrics[SOURCE_METRIC_PREFIX + str(key)] = number
        return metrics

    @classmethod
    def parse_entry(cls, entry: str, stats: ParseStats | None = None) -> ProgressRecord | None:
        """Turn one log entry into a record, or ``None`` if it isn't a valid one."""
        stats = stats if stats is not None else ParseStats()
        marker_at = entry.find(PROGRESS_MARKER)
        if marker_at < 0:
            return None
        stats.progress_entries += 1

        payload = entry[marker_at + len(PROGRESS_MARKER):].lstrip()
        try:
            # raw_decode tolerates trailing text after the JSON document.
            document, _ = json.JSONDecoder().raw_decode(payload)
        except json.JSONDecodeError as exc:
            stats.malformed_json += 1
            stats.note_skip(f"malformed JSON: {exc.msg} at pos {exc.pos}")
            return None
        if not isinstance(document, dict):
            stats.malformed_json += 1
            stats.note_skip("progress payload is not a JSON object")
            return None

        query_id = document.get("id")
        batch_id = _to_batch_id(document.get("batchId"))
        raw_timestamp = document.get("timestamp")
        if not isinstance(query_id, str) or not query_id or batch_id is None:
            stats.missing_required += 1
            stats.note_skip(f"missing/invalid id or batchId (id={query_id!r}, batchId={document.get('batchId')!r})")
            return None
        try:
            timestamp = parse_timestamp(raw_timestamp)
        except ValueError as exc:
            stats.missing_required += 1
            stats.note_skip(f"bad timestamp: {exc}")
            return None

        stats.records += 1
        return {
            "id": query_id,
            "batchId": batch_id,
            "timestamp": timestamp,
            "timestamp_raw": raw_timestamp,
            "metrics": cls.extract_metrics(document),
        }

    def parse_lines(self, lines: Iterable[str], stats: ParseStats | None = None) -> list[ProgressRecord]:
        """Parse a whole file's lines. Duplicates are preserved (see module doc)."""
        stats = stats if stats is not None else ParseStats()
        records: list[ProgressRecord] = []
        for entry in self.iter_entries(lines):
            stats.entries += 1
            record = self.parse_entry(entry, stats)
            if record is not None:
                records.append(record)
        if stats.malformed_json or stats.missing_required:
            logger.warning(
                f"skipped {stats.malformed_json} malformed and {stats.missing_required} incomplete "
                f"progress record(s); samples: {stats.skipped_reasons[:3]}"
            )
        return records

    def parse_text(self, text: str, stats: ParseStats | None = None) -> list[ProgressRecord]:
        return self.parse_lines(text.splitlines(), stats)
