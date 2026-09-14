## File Structure
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
    data/ - Didn't change from init
    seed/ - Didn't change from init
    docker-compose.yml - start the localstask and the autoscaler service
    requirements-dev.txt - requirements for operation + testing
    requirements.txt - minimal requirements for operation
    DESIGN.md - design of the service

## flow
1. main.py: 
1.1 start scaler
2. scaler
2.1 Connect to all AWS services
2.2 Read from SQS, pull messages
2.3 Read from S3 based on message data and starting to fetch the logs
2.4 Run the decision engine on the logs
2.5 Report decisions to DynamoDB
2.6 Waiting for new messages
