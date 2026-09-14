import gzip
import io
import json

import pytest

from autoscaler_service.aws_clients import BotoClientError, S3Client, S3ObjectNotFound


@pytest.fixture
def s3(aws):
    client = S3Client(aws)
    client.client.create_bucket(Bucket=aws.s3_bucket)
    return client


def test_get_json_and_yaml(s3):
    """get_json and get_yaml decode the manifest and rules formats."""
    s3.client.put_object(Bucket=s3.settings.s3_bucket, Key="clusters.json", Body=json.dumps({"clusters": []}))
    s3.client.put_object(Bucket=s3.settings.s3_bucket, Key="config/rules.yaml", Body="rules:\n  - name: r\n")
    assert s3.get_json("clusters.json") == {"clusters": []}
    assert s3.get_yaml("config/rules.yaml") == {"rules": [{"name": "r"}]}


def test_iter_log_lines_gunzips_and_strips_newlines(s3):
    """Gzipped content is decompressed and split into lines without trailing newlines."""
    body = gzip.compress(b"line one\nline two\r\n\nlast")
    s3.client.put_object(Bucket=s3.settings.s3_bucket, Key="logs/c/f.log.gz", Body=body)
    assert list(s3.iter_log_lines("logs/c/f.log.gz")) == ["line one", "line two", "", "last"]


def test_iter_log_lines_plain_text_despite_gz_suffix(s3):
    """Gzip detection uses magic bytes, so an uncompressed .gz file still reads."""
    s3.client.put_object(Bucket=s3.settings.s3_bucket, Key="logs/c/f.log.gz", Body=b"plain\ntext")
    assert list(s3.iter_log_lines("logs/c/f.log.gz")) == ["plain", "text"]


def test_iter_log_lines_replaces_undecodable_bytes(s3):
    """Invalid UTF-8 is replaced instead of aborting the whole file."""
    s3.client.put_object(Bucket=s3.settings.s3_bucket, Key="k", Body=gzip.compress(b"ok\n\xff\xfe bad\n"))
    lines = list(s3.iter_log_lines("k"))
    assert lines[0] == "ok"
    assert "bad" in lines[1]


def test_explicit_bucket_overrides_default(s3):
    """A bucket passed explicitly (from the SQS message) wins over the configured default."""
    s3.client.create_bucket(Bucket="other")
    s3.client.put_object(Bucket="other", Key="k", Body=b"x")
    assert s3.get_object_bytes("k", bucket="other") == b"x"


def test_missing_key_raises_not_found(s3):
    """A missing key raises S3ObjectNotFound, a BotoClientError subclass with code NoSuchKey."""
    with pytest.raises(S3ObjectNotFound) as info:
        s3.get_object_bytes("does/not/exist")
    assert info.value.code == "NoSuchKey"
    assert isinstance(info.value, BotoClientError)


def test_missing_bucket_raises_not_found(aws):
    """A missing bucket is also reported as S3ObjectNotFound."""
    with pytest.raises(S3ObjectNotFound):
        S3Client(aws).get_object_bytes("k", bucket="no-such-bucket-xyz")


def test_iter_log_lines_streams_instead_of_reading_whole_object(settings):
    """A 3 MiB gzip is consumed in bounded chunks: many reads, none anywhere near the full size."""
    payload = gzip.compress(("x" * 100 + "\n").encode() * 30_000)  # ~3 MiB uncompressed

    class CountingBody:
        def __init__(self, data):
            self._buf = io.BytesIO(data)
            self.read_sizes = []

        def read(self, amt=None):
            self.read_sizes.append(amt)
            return self._buf.read(amt)

        def close(self):
            pass

    body = CountingBody(payload)

    class FakeClient:
        def get_object(self, Bucket, Key):
            return {"Body": body}

    client = S3Client(settings, client=FakeClient())
    lines = list(client.iter_log_lines("logs/c/big.log.gz"))
    assert len(lines) == 30_000
    assert len(body.read_sizes) > 1
    assert max(s for s in body.read_sizes if s) < 1024 * 1024
    assert None not in body.read_sizes  # never an unbounded read()


def test_iter_log_lines_closes_body_even_if_consumer_stops_early(settings):
    """Breaking out of the line iterator closes the HTTP body (no leaked connections)."""
    closed = {"v": False}

    class Body:
        def __init__(self):
            self._buf = io.BytesIO(b"a\nb\nc\n")

        def read(self, amt=None):
            return self._buf.read(amt)

        def close(self):
            closed["v"] = True

    class FakeClient:
        def get_object(self, Bucket, Key):
            return {"Body": Body()}

    it = S3Client(settings, client=FakeClient()).iter_log_lines("k")
    assert next(it) == "a"
    it.close()
    assert closed["v"] is True


def test_truncated_gzip_yields_clean_prefix_and_logs_error(s3, caplog):
    """A gzip cut off mid-stream gives back the lines that decoded cleanly instead of raising."""
    full = gzip.compress(("line %d\n" % i for i in range(2000)).__class__ and ("".join(f"line {i}\n" for i in range(2000))).encode())
    s3.client.put_object(Bucket=s3.settings.s3_bucket, Key="logs/c/cut.log.gz", Body=full[: len(full) // 2])
    lines = list(s3.iter_log_lines("logs/c/cut.log.gz"))
    assert 0 < len(lines) < 2000
    assert lines[0] == "line 0"
    assert any("compressed data corrupt" in r.message and r.levelname == "ERROR" for r in caplog.records)
