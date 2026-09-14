#!/usr/bin/env python3
"""Provision the assignment's local AWS resources and load the input data.

Creates (against LocalStack, or any S3/SQS/DynamoDB-compatible endpoint):
  - S3 bucket  `spark-driver-logs` with the log files, clusters.json and config/rules.yaml
  - SQS queue  `driver-log-files` pre-loaded with one message per log file
  - DynamoDB table `scaling_decisions` (cluster_id [S] / decision_ts [S]) — empty

Idempotent: rerun with --reset for a clean slate (recreates queue + table, re-uploads data).
"""
import argparse
import json
import random
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKET = "spark-driver-logs"
QUEUE = "driver-log-files"
TABLE = "scaling_decisions"
REGION = "us-east-1"
SHUFFLE_SEED = 20251103  # deterministic message order across reruns


def make_session(endpoint: str):
    session = boto3.session.Session(
        aws_access_key_id="test", aws_secret_access_key="test", region_name=REGION
    )
    cfg = Config(retries={"max_attempts": 5, "mode": "standard"})
    return (
        session.client("s3", endpoint_url=endpoint, config=cfg),
        session.client("sqs", endpoint_url=endpoint, config=cfg),
        session.client("dynamodb", endpoint_url=endpoint, config=cfg),
    )


def ensure_bucket(s3, reset: bool):
    try:
        s3.create_bucket(Bucket=BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            raise
    if reset:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET):
            objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if objs:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objs})


def ensure_queue(sqs) -> str:
    # Always recreate so the queue contains exactly one fresh message per file.
    try:
        url = sqs.get_queue_url(QueueName=QUEUE)["QueueUrl"]
        sqs.delete_queue(QueueUrl=url)
        time.sleep(1)  # deletion is asynchronous
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("AWS.SimpleQueueService.NonExistentQueue",
                                               "QueueDoesNotExist"):
            raise
    for attempt in range(10):
        try:
            return sqs.create_queue(
                QueueName=QUEUE, Attributes={"VisibilityTimeout": "60"}
            )["QueueUrl"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "AWS.SimpleQueueService.QueueDeletedRecently" \
                    and attempt < 9:
                time.sleep(2)
                continue
            raise


def ensure_table(ddb, reset: bool):
    existing = ddb.list_tables()["TableNames"]
    if TABLE in existing:
        if not reset:
            return
        ddb.delete_table(TableName=TABLE)
        ddb.get_waiter("table_not_exists").wait(TableName=TABLE)
    ddb.create_table(
        TableName=TABLE,
        AttributeDefinitions=[
            {"AttributeName": "cluster_id", "AttributeType": "S"},
            {"AttributeName": "decision_ts", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "cluster_id", "KeyType": "HASH"},
            {"AttributeName": "decision_ts", "KeyType": "RANGE"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    ddb.get_waiter("table_exists").wait(TableName=TABLE)


def upload_data(s3, data_dir: Path, rules_path: Path) -> list:
    manifest_path = data_dir / "clusters.json"
    if not manifest_path.exists():
        sys.exit(f"error: {manifest_path} not found — pass --data-dir")
    s3.upload_file(str(manifest_path), BUCKET, "clusters.json")
    s3.upload_file(str(rules_path), BUCKET, "config/rules.yaml")

    keys = []
    for path in sorted((data_dir / "logs").rglob("*.log.gz")):
        cluster_id = path.parent.name
        key = f"logs/{cluster_id}/{path.name}"
        s3.upload_file(str(path), BUCKET, key)
        keys.append((cluster_id, key))
    return keys


def enqueue(sqs, queue_url: str, keys: list):
    shuffled = list(keys)
    random.Random(SHUFFLE_SEED).shuffle(shuffled)
    for cluster_id, key in shuffled:
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(
                {"bucket": BUCKET, "key": key, "cluster_id": cluster_id}
            ),
        )
    return len(shuffled)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", default="http://localhost:4566")
    parser.add_argument("--data-dir", default=str(Path(__file__).resolve().parent.parent / "data"))
    parser.add_argument("--rules", default=str(Path(__file__).resolve().parent.parent / "rules.yaml"))
    parser.add_argument("--reset", action="store_true",
                        help="wipe bucket contents and recreate the decisions table")
    args = parser.parse_args()

    s3, sqs, ddb = make_session(args.endpoint)
    ensure_bucket(s3, args.reset)
    queue_url = ensure_queue(sqs)
    ensure_table(ddb, args.reset)
    keys = upload_data(s3, Path(args.data_dir), Path(args.rules))
    n = enqueue(sqs, queue_url, keys)

    clusters = json.loads(Path(args.data_dir, "clusters.json").read_text())["clusters"]
    print(f"bucket  s3://{BUCKET}: {n} log files, clusters.json, config/rules.yaml")
    print(f"queue   {queue_url}: {n} messages")
    print(f"table   {TABLE}: ready (empty)")
    print(f"clusters: " + ", ".join(
        f"{c['cluster_id']} (workers={c['initial_workers']}, files={c['num_log_files']})"
        for c in clusters))


if __name__ == "__main__":
    main()
