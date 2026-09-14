"""S3: fetch the manifest, the rules file and the (gzipped) driver logs."""
from __future__ import annotations

import gzip
import io
import json
import logging
import zlib
from typing import Any, Iterator

import yaml

from autoscaler_service.aws_clients.base_boto_client import BaseBotoClient, BotoClientError

logger = logging.getLogger(__name__)

GZIP_MAGIC = b"\x1f\x8b"


class S3ObjectNotFound(BotoClientError):
    """The key (or bucket) does not exist. Not retryable."""


class S3Client(BaseBotoClient):
    service_name = "s3"

    def _bucket(self, bucket: str | None) -> str:
        return bucket or self._settings.s3_bucket

    def get_object_bytes(self, key: str, bucket: str | None = None) -> bytes:
        """Small objects only (manifest, rules); logs go through :meth:`iter_log_lines`."""
        with self.open_object(key, bucket) as body:
            return body.read()

    def get_text(self, key: str, bucket: str | None = None, encoding: str = "utf-8") -> str:
        return self.get_object_bytes(key, bucket).decode(encoding)

    def get_json(self, key: str, bucket: str | None = None) -> Any:
        return json.loads(self.get_text(key, bucket))

    def get_yaml(self, key: str, bucket: str | None = None) -> Any:
        return yaml.safe_load(self.get_text(key, bucket))

    def ensure_object_exists(self, key: str, bucket: str | None = None) -> int:
        """``HEAD`` the object; returns its size. Raises :class:`S3ObjectNotFound` so a
        notification for a missing file can be rejected before it is remembered."""
        bucket = self._bucket(bucket)
        try:
            return int(self._call("head_object", Bucket=bucket, Key=key)["ContentLength"])
        except BotoClientError as exc:
            if exc.code in ("NoSuchKey", "NoSuchBucket", "404", "NotFound"):
                raise S3ObjectNotFound(
                    f"s3://{bucket}/{key} does not exist", code=exc.code, operation=exc.operation
                ) from exc
            raise

    def open_object(self, key: str, bucket: str | None = None) -> Any:
        """Return the raw botocore ``StreamingBody`` for ``key`` (caller closes it)."""
        bucket = self._bucket(bucket)
        try:
            return self._call("get_object", Bucket=bucket, Key=key)["Body"]
        except BotoClientError as exc:
            if exc.code in ("NoSuchKey", "NoSuchBucket", "404", "NotFound"):
                raise S3ObjectNotFound(
                    f"s3://{bucket}/{key} does not exist", code=exc.code, operation=exc.operation
                ) from exc
            raise

    def iter_log_lines(self, key: str, bucket: str | None = None) -> Iterator[str]:
        """Yield the decoded lines of a log file, streaming and transparently gunzipping.

        The object is never held in memory as a whole: bytes flow from the HTTP
        response through a buffered reader, ``GzipFile`` and ``TextIOWrapper`` one
        block at a time, so memory use is bounded by the buffer sizes rather than by
        the file size. Detection is by magic bytes rather than the ``.gz`` suffix so a
        mislabelled file still parses. Undecodable bytes are replaced instead of
        raising: a corrupt byte in some unrelated log entry must not sink the whole
        file. Lines are yielded without their trailing newline.
        """
        body = self.open_object(key, bucket)
        lines = 0
        try:
            buffered = io.BufferedReader(_StreamingBodyReader(body))
            raw: io.BufferedIOBase = gzip.GzipFile(fileobj=buffered) if buffered.peek(2)[:2] == GZIP_MAGIC else buffered
            with io.TextIOWrapper(raw, encoding="utf-8", errors="replace") as text:
                for line in text:
                    lines += 1
                    yield line.rstrip("\r\n")
        except (EOFError, gzip.BadGzipFile, zlib.error) as exc:
            # A truncated or corrupt compressed tail is the compressed-level twin of
            # "a file rotated mid-entry": keep what decoded cleanly, report loudly, and
            # let the file count as processed. Raising here would leave the message
            # in the queue and retry the same corrupt bytes forever.
            logger.error(f"s3://{self._bucket(bucket)}/{key}: compressed data corrupt after {lines} line(s) ({exc}); using the lines read so far")
        finally:
            body.close()


class _StreamingBodyReader(io.RawIOBase):
    """Adapt botocore's ``StreamingBody`` (which only offers ``read(n)``) to the
    ``RawIOBase`` interface so it can sit under ``io.BufferedReader``/``GzipFile``."""

    def __init__(self, body: Any):
        super().__init__()
        self._body = body

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray) -> int:  # type: ignore[override]
        chunk = self._body.read(len(buffer))
        buffer[: len(chunk)] = chunk
        return len(chunk)
