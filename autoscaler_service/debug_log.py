"""Debug-mode trace files (``DEBUG_MODE=true``).

Two append-only JSON-lines files are written under ``DEBUG_LOG_DIR``:

* ``sqs_messages.log`` — every message received from the queue, with the wall-clock
  time it was read and its raw body.
* ``decisions.log`` — every evaluation point at which some rule's condition held,
  with the aggregate vs. threshold comparison, the window statistics and the gate
  outcome (decision made, or blocked by idempotence / cooldown).

One JSON object per line keeps the files greppable and trivially loadable with
``pandas``/``jq``. Writes are serialised with a lock because several worker threads
may log concurrently; each write opens, appends and closes so a crash never leaves
a half-written buffer behind.

Debug output must never affect the real work: any I/O failure is logged once, the
writer disables itself, and the scaler carries on. The container runs as root and
the directory is bind-mounted from the host, so files are created world-writable
to let the host user read and delete them.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from autoscaler_service.aws_clients import QueueMessage

logger = logging.getLogger(__name__)

SQS_MESSAGES_FILE = "sqs_messages.log"
DECISIONS_FILE = "decisions.log"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class DebugLogger:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.sqs_messages_path = self.directory / SQS_MESSAGES_FILE
        self.decisions_path = self.directory / DECISIONS_FILE
        self._lock = threading.Lock()
        self._disabled = False
        _chmod_quietly(self.directory, 0o777)
        logger.info(f"debug mode: writing traces to {self.sqs_messages_path} and {self.decisions_path}")

    @property
    def disabled(self) -> bool:
        """True after a write failure; further calls are no-ops."""
        return self._disabled

    def log_sqs_message(self, message: QueueMessage) -> None:
        """Record one received queue message. The body is stored both raw and, when
        it is valid JSON, decoded — so a malformed body is still visible verbatim."""
        try:
            body: Any = json.loads(message.body)
        except json.JSONDecodeError:
            body = None
        self._append(
            self.sqs_messages_path,
            {
                "read_at": _now_iso(),
                "message_id": message.message_id,
                "body": body,
                "raw_body": message.body,
            },
        )

    def log_decision_trace(self, event: dict[str, Any]) -> None:
        """Record one rule evaluation event produced by ``DecisionEngine``."""
        self._append(self.decisions_path, {"logged_at": _now_iso(), **event})

    def _append(self, path: Path, record: dict[str, Any]) -> None:
        if self._disabled:
            return
        try:
            line = json.dumps(record, default=str)
            with self._lock:
                is_new = not path.exists()
                with path.open("a", encoding="utf-8") as fh:
                    fh.write(line + "\n")
                if is_new:
                    _chmod_quietly(path, 0o666)
        except (OSError, TypeError, ValueError) as exc:
            # Debug tracing is best-effort: report once, then stop trying.
            self._disabled = True
            logger.warning(f"debug trace disabled after failing to write {path}: {exc}")


def _chmod_quietly(path: Path, mode: int) -> None:
    try:
        os.chmod(path, mode)
    except OSError:
        pass  # e.g. not the owner; the write itself will tell us if it matters
