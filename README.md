# Backend Home Assignment — Streaming Job Autoscaler

Welcome! In this assignment you'll build a small autoscaling service for spark-streaming jobs. It's a service that reads Apache Spark
driver logs, extracts streaming-job performance metrics from them, and decides when compute
clusters should be scaled up or down.

This is a miniature version of a real production system we run at Zipher. You are **not**
expected to know AWS or Spark — everything you need is explained below, and learning the
few AWS basics involved (S3, SQS, DynamoDB, all running locally via LocalStack) is part of
the assignment.

**Estimated effort:** ~3–6 hours (including AWS reading time).

---

## 1. Background

Our customers run *streaming jobs*: long-lived Spark applications that continuously pull
data from a queue of incoming files and process it in small batches. Each job runs on its own cluster, which includes one driver node and one or more worker nodes.

- Too **few** workers → batches take too long, unprocessed data ("backlog") piles up.
- Too **many** workers → the customer pays for machines that sit idle.

The Spark *driver* (the coordinator node of each cluster) writes a log file. In those log files 
the driver periodically writes a **progress report**: a JSON document
describing the batch it just finished — how long it took, how many rows it processed, and
how much backlog remains at the source.

Your service ingests these log files, extracts the progress reports, evaluates a set of
**scaling rules** against them, and records **scaling decisions**.

## 2. What you build

A containerized Python service that:

1. Consumes file-notification messages from an **SQS queue**. Each message points at one
   driver log file stored in **S3**.
2. Downloads and parses each log file, extracting the streaming progress records.
3. Evaluates the scaling rules (provided as a YAML file in S3) against each cluster's
   records, exactly as specified in section 6.
4. Writes the resulting scaling decisions to a **DynamoDB table**.
5. When all files of all clusters have been processed and all decisions written, prints
   `ALL CLUSTERS PROCESSED` to stdout (our grading harness waits for this line).

In a real world scenario, the log files would be processed in real-time as the driver writes them. In this excercise you will process static log files and create a stream of scaling decision that 
simulate the scaling pattern we'd create for that cluster. 

All AWS resources are **local** — they're served by [LocalStack](https://docs.localstack.cloud/)
from the provided `docker-compose.yml`. You do not need a real AWS account.

## 3. Environment setup — step by step

You need **Python 3.10+** and (preferably) **Docker with the Compose plugin**.

### 3.1 Install the prerequisites

- **macOS / Windows:** install [Docker Desktop](https://docs.docker.com/desktop/) and
  start it. Verify with `docker --version` and `docker compose version`.
- **Linux:** install [Docker Engine](https://docs.docker.com/engine/install/) for your
  distro, e.g. Ubuntu: `sudo apt-get install docker.io docker-compose-v2`, then
  `sudo usermod -aG docker $USER` and re-login. Verify: `docker run --rm hello-world`.
- **Python:** any 3.10+ works: `python3 --version`.

### 3.2 Start LocalStack

From the assignment's root directory (where `docker-compose.yml` is):

```bash
docker compose up -d
# wait until healthy, then confirm S3/SQS/DynamoDB are "available":
docker compose ps
curl -s http://localhost:4566/_localstack/health
```

LocalStack is a local AWS emulator: one container that serves S3, SQS and DynamoDB on
`http://localhost:4566`. Nothing leaves your machine and no AWS account is involved.

### 3.3 Load the input data

```bash
python3 -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r seed/requirements.txt
python seed/seed.py
```

Expected output: a summary listing the bucket, the queue with one message per log file,
the empty `scaling_decisions` table, and the clusters with their initial worker counts.
The seed script is idempotent — rerun it with `--reset` any time you want a clean slate.

### 3.4 Verify you can reach everything (optional but recommended)

No AWS CLI needed — a Python one-liner per resource:

```bash
python -c "import boto3; s3=boto3.client('s3',endpoint_url='http://localhost:4566',region_name='us-east-1',aws_access_key_id='test',aws_secret_access_key='test'); print(*[o['Key'] for o in s3.list_objects_v2(Bucket='spark-driver-logs')['Contents'][:8]], sep='\n')"

python -c "import boto3; sqs=boto3.client('sqs',endpoint_url='http://localhost:4566',region_name='us-east-1',aws_access_key_id='test',aws_secret_access_key='test'); q=sqs.get_queue_url(QueueName='driver-log-files')['QueueUrl']; print(sqs.get_queue_attributes(QueueUrl=q,AttributeNames=['ApproximateNumberOfMessages'])['Attributes'])"
```

If you prefer the AWS CLI: `pip install awscli`, then any command works with
`aws --endpoint-url http://localhost:4566 ...` and env vars
`AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1`.

### 3.5 Connecting from your code

Your service must talk to AWS through the endpoint `http://localhost:4566`
(credentials: access key `test`, secret `test`, region `us-east-1`). In boto3:

```python
import boto3
s3 = boto3.client("s3", endpoint_url="http://localhost:4566", region_name="us-east-1",
                  aws_access_key_id="test", aws_secret_access_key="test")
```

### 3.6 Troubleshooting

- **`port 4566 already allocated`** — something else uses the port; stop it or change the
  mapping in `docker-compose.yml` (and use the new port everywhere).
- **`Unable to locate credentials`** — pass the dummy credentials explicitly (see 3.5);
  don't rely on a real `~/.aws` profile.
- **Seed says `error: ... clusters.json not found`** — run it from the assignment root,
  or pass `--data-dir path/to/data`.
- **Queue looks empty after experimenting** — messages you received but didn't delete
  reappear after the 60s visibility timeout; `python seed/seed.py --reset` restores a
  pristine state at any time.

### 3.7 No Docker? There's a fallback

If you cannot install Docker (e.g. a locked-down machine), you can run
[moto](https://docs.getmoto.org/en/latest/docs/server_mode.html) — a pip-installable AWS
emulator that serves the same APIs on the same kind of endpoint:

```bash
pip install 'moto[server]'
moto_server -p 4566          # instead of `docker compose up -d`
python seed/seed.py          # everything else is identical
```

Docker remains the recommended path.

## 4. The inputs

### 4.1 Resources created by the seed script

| Resource | Name | Contents |
|---|---|---|
| S3 bucket | `spark-driver-logs` | Log files under `logs/<cluster_id>/`, manifest at `clusters.json`, rules at `config/rules.yaml` |
| SQS queue | `driver-log-files` | One message per log file (see 4.2) |
| DynamoDB table | `scaling_decisions` | Empty — your service writes it (see 7) |

### 4.2 SQS messages

The queue is pre-loaded with one message per log file, in **no particular order** — files
of different clusters are interleaved and a cluster's files are *not* announced in
chronological order. Message body:

```json
{"bucket": "spark-driver-logs", "key": "logs/cluster-aurora/log4j-2025-11-03-09.log.gz", "cluster_id": "cluster-aurora"}
```

Consume messages properly: receive, process, **delete**. The manifest (4.3) tells you how
many files each cluster has, so you know when you've seen everything.

### 4.3 The manifest — `s3://spark-driver-logs/clusters.json`

```json
{
  "clusters": [
    {"cluster_id": "cluster-aurora", "initial_workers": 4, "num_log_files": 5},
    ...
  ]
}
```

`initial_workers` is the cluster's worker count before any of your decisions.
Each cluster runs **exactly one** streaming query.

### 4.4 Driver log files

Each file is a gzipped text file, rotated hourly (`log4j-YYYY-MM-DD-HH.log.gz`).
Log entries start with a timestamp header line:

```
25/11/03 09:14:00 INFO SomeLoggerName: message text
```

**A log entry may span multiple lines** (stack traces, pretty-printed JSON, config dumps):
every line up to the next timestamp-headed line belongs to the same entry.

The entries you care about contain the marker `Streaming query made progress:` followed by
a JSON document. Both of these appear in the wild — treat them identically:

```
25/11/03 09:14:00 INFO ProgressReporter: Streaming query made progress: {"id":"a91c...","runId":"77b0...","name":"ingest_events_aurora","timestamp":"2025-11-03T09:14:00.000Z","batchId":247,"batchDuration":78450,"numInputRows":152340, ...}
```

```
25/11/03 09:14:00 INFO MicroBatchExecution: Streaming query made progress: {
  "id" : "a91c...",
  "runId" : "77b0...",
  "timestamp" : "2025-11-03T09:14:00.000Z",
  "batchId" : 247,
  ...
}
```

A full progress record looks like this (fields you don't need are omitted here but present
in the files — parse leniently):

```json
{
  "id": "a91c1e2f-...",              // streaming query id (stable for the query's lifetime)
  "runId": "77b0c3d4-...",
  "name": "ingest_events_aurora",
  "timestamp": "2025-11-03T09:14:00.000Z",   // batch trigger time, UTC — this is "the record's timestamp"
  "batchId": 247,
  "batchDuration": 78450,            // ms; MAY BE ABSENT on some records
  "numInputRows": 152340,
  "inputRowsPerSecond": 1269.5,
  "processedRowsPerSecond": 1941.9,
  "durationMs": {"addBatch": 71234, "triggerExecution": 78450, ...},
  "stateOperators": [],
  "sources": [
    {
      "description": "CloudFilesSource[s3://zipher-demo/events/aurora]",
      "startOffset": {"seqNum": 128872},
      "endOffset": {"seqNum": 129024},
      "numInputRows": 152340,
      "inputRowsPerSecond": 1269.5,
      "processedRowsPerSecond": 1941.9,
      "metrics": {
        "numBytesOutstanding": "5388009472",      // NOTE: source metrics are strings
        "numFilesOutstanding": "1287",
        "approximateQueueSize": "1287"
      }
    }
  ],
  "sink": {"description": "DeltaSink[...]", "numOutputRows": 152340}
}
```

**These are real-world logs. Expect, and handle gracefully:**

- Plenty of unrelated log entries, including multi-line ones, and lines that *almost* look
  relevant (e.g. `Streaming query has been idle...`). Only entries with the exact marker
  `Streaming query made progress:` are progress records.
- **Duplicates**: the same batch may be reported more than once — twice within one file
  (different log formats) and/or again in the next file (rotation overlap). Duplicate
  copies carry identical values. Deduplicate by `(id, batchId)`.
- **Malformed records**: a progress entry whose JSON does not parse (e.g. an interrupted
  write, or a file that was rotated mid-entry), or that lacks any of the required fields
  `id`, `batchId`, `timestamp`. **Skip such records**; never let them crash the run.
- **Missing metrics**: a record that parses fine but lacks a particular metric (e.g. no
  `batchDuration`). The record is still valid — see section 6 for how it's treated.

## 5. The rules file — `s3://spark-driver-logs/config/rules.yaml`

```yaml
rules:
  - name: scale_up_backlog_surge
    metric: source.numBytesOutstanding
    aggregation: last
    operator: gt
    threshold: 5000000000
    window_minutes: 10
    min_batches: 3
    target_workers: 16
    cooldown_minutes: 20
  # ... (see the actual file for the full list)
```

Field meanings:

| Field | Meaning |
|---|---|
| `metric` | Which value to read from each progress record. `batchDuration`, `numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond` are top-level fields; `source.<name>` means `sources[0].metrics.<name>`, parsed from string to number. |
| `aggregation` | How to combine the metric over the window: `avg`, `max`, `min`, or `last` (the value from the most recent record in the window). |
| `operator` | `gt` or `lt`, both strict. |
| `threshold` | The number the aggregate is compared against. |
| `window_minutes` | Look-back window size (see 6.3). |
| `min_batches` | Minimum number of records *carrying this metric* required in the window for the rule to be evaluable. |
| `target_workers` | The worker count this rule scales the cluster to. |
| `cooldown_minutes` | Minimum time since the cluster's previous decision before this rule may fire (see 6.4). |

Your service must read this file from S3 — don't bundle a copy into your image; we grade
with the same file but different log data.

## 6. Decision semantics — follow these exactly

Your output is compared against ours record-for-record, so the semantics below are
normative. When in doubt, re-read this section; every sentence is deliberate.

### 6.1 Record set

Per cluster: take all valid progress records from all of the cluster's files, dedup by
`(id, batchId)`, and sort ascending by `(timestamp, batchId)`. All timestamps are the
records' JSON `timestamp` field (UTC) — **log-line prefix timestamps and wall-clock time
play no role anywhere in this assignment.**

### 6.2 Evaluation points

Walk the cluster's sorted records one by one. Each record — including records that are
missing some metric — is an evaluation point. Let `T` be the current record's timestamp.
At each evaluation point, find the **first rule in file order whose condition holds**
(6.3). If no rule's condition holds, move on. If one does, it is *the matched rule* —
rules below it are not considered for this evaluation point, regardless of what the gates
in 6.4 decide.

### 6.3 Does a rule's condition hold at time T?

1. Collect the cluster's records with timestamp `ts` satisfying
   **`ts > T − window_minutes` and `ts ≤ T`** (strictly greater on the left — a record
   exactly `window_minutes` old is *outside* the window).
2. Of those, keep the records that carry the rule's metric (present and parseable).
3. If fewer than `min_batches` records remain → the condition does **not** hold.
4. Aggregate the metric values (`avg` / `max` / `min`; `last` = the value from the record
   with the greatest `(timestamp, batchId)` among those kept).
5. The condition holds iff `aggregate OP threshold` (strict `>` / `<`).

### 6.4 Gates — does the matched rule produce a decision?

Track two pieces of per-cluster state while walking the records:
`current_workers` (starts at the manifest's `initial_workers`) and the timestamp of the
cluster's most recent decision, `last_decision_ts` (starts unset).

The matched rule produces a decision unless either gate blocks it:

- **Idempotence**: if `target_workers == current_workers` → no decision.
- **Cooldown**: if `last_decision_ts` is set and
  **`T < last_decision_ts + cooldown_minutes`** (the *matched rule's* cooldown) → no
  decision. Note the strict `<`: a matched rule exactly `cooldown_minutes` after the
  previous decision **does** fire.

If a decision is produced: record it (section 7), set `current_workers = target_workers`
and `last_decision_ts = T`, and continue walking the records.

### 6.5 Worked example

Rules: `A` (avg batchDuration gt 300000, window 15, min 3, target 8, cooldown 20) placed
above `B` (max numInputRows lt 1000, window 30, min 5, target 2, cooldown 60).
Cluster starts at 4 workers. Batches arrive every 2 minutes.

- `09:40` — window (09:25, 09:40] has 8 records with batchDuration, avg 344000 > 300000.
  A is matched; 8 ≠ 4, no previous decision → **decision: 4 → 8** at `09:40`, rule A.
- `09:42` — A matches again (avg still high) but target 8 == current 8 → idempotence gate,
  nothing recorded. B is *not* considered (A was the matched rule).
- `09:56` — suppose A's condition holds. Cooldown: 09:56 < 09:40 + 20min → blocked.
- `10:00` — A's condition holds, 10:00 == 09:40 + 20min → cooldown passed... but target
  8 == current 8 → still nothing. (Had the cluster meanwhile been scaled elsewhere, this
  would have fired.)

## 7. The output — DynamoDB table `scaling_decisions`

The table is pre-created by the seed script with partition key `cluster_id` (string) and
sort key `decision_ts` (string). For every decision, write one item:

| Attribute | Type | Value |
|---|---|---|
| `cluster_id` | S | the cluster |
| `decision_ts` | S | the triggering record's JSON `timestamp` string, **verbatim** (e.g. `2025-11-03T09:40:30.000Z`) |
| `rule_name` | S | the matched rule's `name` |
| `from_workers` | N | worker count before this decision |
| `to_workers` | N | worker count after this decision |

Write **only** genuine decisions — extra, missing, or altered items all count against
correctness. Clusters with no decisions simply have no items.


## 10. Deliverables

A git repository (or zip) containing:

- Your service source code + `Dockerfile`, integrated into the provided
  `docker-compose.yml` (add your service; don't modify the LocalStack service or the seed
  script).
- `DESIGN.md`.
- A `README` section with the exact commands to run it — running your solution must take
  at most: `docker compose up -d --build`, `python seed/seed.py`, and (if your service is
  not part of compose) one documented run command.
- Whatever tests you found worth writing. We don't require coverage; we do read tests as
  a signal of what you considered fragile.

## 11. How we evaluate

Roughly in this order:

1. **Correctness** — we run your service against a *different* dataset (same format, same
   rules file) and diff the `scaling_decisions` table against the expected output.
2. **Code quality** — clarity, structure, naming, error handling; the parsing code gets
   read closely.
3. **DESIGN.md** — depth and honesty of the scale-up and failure-recovery answers.
4. **AWS usage** — idiomatic-enough use of SQS (receive/delete), S3, DynamoDB.

Notes:

- You may use AI assistants the way you would on the job. Two constraints: you must
  deeply understand every line you submit (the follow-up interview digs into your code
  and *why* it is the way it is), and DESIGN.md must be written by you, in your own words.
- If something in the spec seems ambiguous, make a reasonable call and document it in
  DESIGN.md — but check section 6 first; most questions are answered there.

Good luck — we hope you have fun with it!

## 12. Running the service

```bash
# edit autoscaler_service/config.py to change service configuration
python3 -m venv .venv                # Create venv
source .venv/bin/activate            # Start .venv
pip3 install -r requirements-dev.txt # Install dev requirements for testing
docker compose up -d --build         # LocalStack + the autoscaler (waits for the seed)
python seed/seed.py                  # load data; processing starts automatically
docker compose logs -f autoscaler    # ends with "ALL CLUSTERS PROCESSED"
docker compose down                  # Stop and remove the services
```

### Other useful commands and API
- **System health and config**: `GET http://localhost:8000/health`, `/rules` and `/clusters` show the service state.
- **Tune throughput** with `NUM_OF_WORKERS=4 SQS_MAX_MESSAGES=10 docker compose up -d --build`.
- **Tests**: `pip install -r requirements-dev.txt && pytest`.
- **Read formatted dicions**: `python -c "import boto3; d=boto3.client('dynamodb',endpoint_url='http://localhost:4566',region_name='us-east-1',aws_access_key_id='test',aws_secret_access_key='test'); [print(i['cluster_id']['S'], i['decision_ts']['S'], i['rule_name']['S'], i['from_workers']['N'], '->', i['to_workers']['N']) for i in sorted(d.scan(TableName='scaling_decisions')['Items'], key=lambda i:(i['cluster_id']['S'], i['decision_ts']['S']))]"`
- **For the rest of the list** Please install AWS CLI client
- **List documents in DynamoDB**: `aws dynamodb scan --table-name scaling_decisions --endpoint-url http://localhost:4566`
- **Check number of messages in SQS**: `aws sqs get-queue-attributes --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/driver-log-files --attribute-names All --endpoint-url http://localhost:4566`
- **Check S3 files**: `aws s3 ls s3://spark-driver-logs --recursive --endpoint-url http://localhost:4566`
