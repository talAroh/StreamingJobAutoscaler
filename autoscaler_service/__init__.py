"""Streaming Job Autoscaler service.

Consumes S3 file notifications from SQS, parses Spark driver logs, evaluates the
scaling rules from README section 6 and writes decisions to DynamoDB.
"""
