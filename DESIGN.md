## System Overview
In this project I implemented the requested service in a python package called `autoscaler_service`.
This package runs in a container loaded by docker compose, and expose port `8000` for basic service management operations.

### Important note
**The `Running the service` guide in located on section 12 in README.md**

### Key assumptions and decisions
1. Data record is never split across two files. When rotation does clip a record, log4j starts the next file with a new entry
2. S3 bucket files are not deleted at the end (intentional)
3. I review the logs in chronological order so the decision process will make more sense and to save memory
4. Section 6 requires walking records in (timestamp, batchId) order, and a record's timestamp is when the batch was triggered, not when it was logged. A batch triggered at 08:59:30 that finishes at 09:00:01 is written into the 09:00 file. When only the 08:00 file has been read, its last records cannot be evaluated, because a record that belongs before them in the walk may still be sitting in the next file. Evaluating early would put that spilled record's evaluation point after the 09:xx points, so the cooldown and worker-count state would advance in the wrong order, and any 08:5x point would compute its window without a record that is inside it.

### Scale-up
This Service is designed to scale-up for each AWS service:
- SQS scaling - In that case we can do:
    - Raise the number of workers in scaler (`NUM_OF_WORKERS` in service config)
    - Increase the number of `SQS_MAX_MESSAGES` in service config
- S3 log reading scaling - Raise the number of workers in scaler (NUM_OF_WORKERS in service config)
- DynamoDB decision writing scaling - Increase `DYNAMODB_BATCH_SIZE` in service config or raise the number of workers in scaler (`NUM_OF_WORKERS` in service config)

### Failure handling and recovery
#### SQS
| Scenario                                             | What happens                                                                |
|------------------------------------------------------|-----------------------------------------------------------------------------|
| Cannot connect, queue missing, LocalStack restarting | Receive fails with a warning, cached URL is dropped, retry every 3s forever |
| Body not JSON, not an object, missing or empty field | Logged as error, acked and dropped so it cannot poison the queue            |
| Cluster not in manifest                              | Logged, acked and dropped                                                   |
| Redelivery of an already processed file              | Recognized by the file set, acked, no double counting                       |
| Delete fails after successful processing             | Warning; the redelivered copy is a harmless duplicate                       |
| Batch slower than the 60s visibility timeout         | Redelivery while in flight, wasted work, no wrong output                    |

#### S3
| Scenario                                        | What happens |
|-------------------------------------------------|-------------------------------------------------------------------------------------------------------------------|
| Object or bucket missing                        | Logged, acked and dropped                                                                                         |
| Cannot connect when fetching                    | Not acked, redelivered later                                                                                      |
| Connection drops mid-stream                     | Not acked, redelivered later, but logged as "unexpected error" with a traceback rather than the clean AWS warning |
| Truncated or corrupt gzip                       | Clean prefix kept, ERROR logged, file counts as processed                                                         |
| Plain text despite `.gz`, bad UTF-8, empty file | Handled: magic-byte detection, replacement chars, zero records                                                    |
| Manifest or rules missing at startup            | Retry every 3s until the seed runs                                                                                |
| Manifest or rules malformed or invalid          | Logged, `/health` returns 503                                                                                     |

#### DynamoDB
| Scenario                                                      | What happens                                                                                    |
|---------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| Write fails, table missing, throttled past botocore's retries | Not acked, cluster re-evaluated on redelivery, `put_item` idempotent so partial writes are safe |
| Crash between last write and ack                              | Same replay path, no duplicates                                                                 |


## File Structure
```text
├── autoscaler_service
│   ├── aws_clients
│   │   ├── base_boto_client.py
│   │   ├── dynamodb_client.py
│   │   ├── __init__.py
│   │   ├── s3_client.py
│   │   └── sqs_client.py
│   ├── config.py
│   ├── debug_log.py
│   ├── Dockerfile
│   ├── engine
│   │   ├── decision_engine.py
│   │   ├── __init__.py
│   │   └── scaler.py
│   ├── __init__.py
│   ├── main.py
│   ├── models
│   │   ├── __init__.py
│   │   └── models.py
│   ├── parsers
│   │   ├── __init__.py
│   │   └── spark_log_parser.py
│   └── tests
│       ├── conftest.py
│       ├── __init__.py
│       ├── test_aws_clients
│       │   ├── __init__.py
│       │   ├── test_base_boto_client.py
│       │   ├── test_dynamodb_client.py
│       │   ├── test_s3_client.py
│       │   └── test_sqs_client.py
│       ├── test_config.py
│       ├── test_engine
│       │   ├── __init__.py
│       │   ├── test_decision_engine.py
│       │   └── test_scaler.py
│       ├── test_main.py
│       └── test_parsers
│           ├── __init__.py
│           └── test_spark_log_parser.py
├── data
│   ├── clusters.json
│   └── logs
│       ├── cluster-aurora
│       ├── cluster-borealis
│       ├── cluster-cascade
│       ├── cluster-drift
│       └── cluster-ember
├── DESIGN.md
├── docker-compose.yml
├── logs
├── pytest.ini
├── README.md
├── requirements-dev.txt
├── requirements.txt
├── rules.yaml
└── seed
    ├── requirements.txt
    └── seed.py
```

### Key files explanation
    autoscaler_service/
        main.py - entry point that start the scaler object and read the rules/clusters files from the S3 bucket + A FastAPI server with the following endpoints (on port 8000 HTTP):
                  health - GET request with no params to check the system is working
                  rules - GET request with no params that return the loaded rules
                  clusters - GET request with no params that return the loaded clusters
        Dockerfile - a Container for the autoscaler services
        config.py - contains all the system config keys + values
        engine/
            decision_engine.py - Make the decisions based on the rules
            scaler.py - A main class to run the workers that perform the logic:
                        Read notification from SQS
                        Read the file from the S3 (based on the file name from the SQS message)
                        Parse the logs
                        Check for a decision
                        Write decision to DB (if the system needs to make one)
        models/
            models.py - contain the ODM + constraints for the DynamoDB document model
        aws_clients/
            base_boto_client.py - handle generic and common boto client logic
            sqs_client.py - handle specific connection + operations on a local SQS
                            operations:
                            read_msg_from_queue
                            ack_message_processed
            s3_client.py - handle specific connection + operations on a local S3
            dynamodb_client.py - handle specific connection + operations on a local DynamoDB
        parsers/
            spark_log_parser.py - helper class to parse the metrics from Spark logs
        tests/
            test_engines/
                test_scaler.py - pytest tests with basic unit tests
                test_decision_engine.py - pytest tests with basic unit tests
            test_aws_clients/
                test_base_boto_client.py - pytest tests with basic unit tests
                sqs_client.py - pytest tests with basic unit tests
                s3_client.py - pytest tests with basic unit tests
                dynamodb_client.py - pytest tests with basic unit tests
            test_parsers/
                test_spark_log_parser.py - pytest tests with basic unit tests
    docker-compose.yml - start the localstask and the autoscaler service
    requirements-dev.txt - requirements for operation + testing
    requirements.txt - minimal requirements for operation
    DESIGN.md - design of the service

## System Flow
- main.py (entry point):
    - Read configuration, rules and manifest
    - Connect to all AWS services
    - Start scaler
- scaler.py (service flow manager)
    - Create and run workers threadpool
    - For each worker
        - Read messages from SQS
        - Read log file from S3 based on message data and starting to gather and parse the logs
        - Run the decision engine on the parsed logs
        - Report decisions to DynamoDB
        - Waiting for new messages


## Data Model & Storage
For the decision documents I decided to use an ODM based on pydantic BaseModel called ScalingDecision

## API
- `GET http://localhost:8000/health # Show system health status + number of workers`
    - Return `200` when the system is healthy, `503` otherwise
- `GET http://localhost:8000/rules # Show system rules loaded from S3`
    - Return `200` when the system has a rules file loaded, `503` otherwise
- `GET http://localhost:8000/clusters # Show system clusters loaded from S3`
    - Return `200` when the system has a rules file loaded, `503` otherwise
