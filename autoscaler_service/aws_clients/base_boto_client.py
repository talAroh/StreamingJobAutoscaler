"""Shared plumbing for the boto3-backed clients.

Responsibilities kept here so the service clients stay small:

* build a boto3 client from :class:`Settings` (endpoint, dummy credentials, region,
  retry policy) — every subclass only declares its ``service_name``;
* translate botocore's exception zoo into one :class:`BotoClientError` that carries
  the AWS error code, so callers can branch on ``exc.code == "NoSuchKey"`` without
  importing botocore themselves;
* allow a pre-built client to be injected, which is how tests swap in moto/fakes.
"""
from __future__ import annotations

import logging
from typing import Any, ClassVar

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from autoscaler_service.config import Settings

logger = logging.getLogger(__name__)


class BotoClientError(RuntimeError):
    """Any failure talking to AWS. ``code`` is the AWS error code when known."""

    def __init__(self, message: str, *, code: str | None = None, operation: str | None = None):
        super().__init__(message)
        self.code = code
        self.operation = operation


class BaseBotoClient:
    #: boto3 service name, e.g. ``"s3"``. Subclasses must set it.
    service_name: ClassVar[str]

    def __init__(self, settings: Settings, client: Any | None = None):
        if not getattr(self, "service_name", None):
            raise TypeError(f"{type(self).__name__} must define service_name")
        self._settings = settings
        self._client = client if client is not None else self._build_client()

    # -- construction -----------------------------------------------------------

    def _client_config(self) -> Config:
        """Retry/timeout policy. Long-polling SQS needs a read timeout above the
        wait time, so we size it from settings instead of hard-coding."""
        return Config(
            retries={"max_attempts": 5, "mode": "standard"},
            connect_timeout=5,
            read_timeout=max(30, self._settings.sqs_wait_time_seconds + 10),
        )

    def _build_client(self) -> Any:
        session = boto3.session.Session(
            aws_access_key_id=self._settings.aws_access_key_id,
            aws_secret_access_key=self._settings.aws_secret_access_key,
            region_name=self._settings.aws_region,
        )
        return session.client(
            self.service_name,
            endpoint_url=self._settings.aws_endpoint_url,
            config=self._client_config(),
        )

    # -- accessors ---------------------------------------------------------------

    @property
    def client(self) -> Any:
        """The underlying boto3 client, for operations not wrapped here."""
        return self._client

    @property
    def settings(self) -> Settings:
        return self._settings

    # -- error handling ----------------------------------------------------------

    @staticmethod
    def error_code(exc: ClientError) -> str | None:
        return exc.response.get("Error", {}).get("Code")

    def _call(self, operation: str, **kwargs: Any) -> Any:
        """Invoke ``client.<operation>(**kwargs)`` and normalise failures.

        Retries for throttling / transient network errors are handled by botocore's
        ``standard`` retry mode; what reaches here is final.
        """
        try:
            return getattr(self._client, operation)(**kwargs)
        except ClientError as exc:
            code = self.error_code(exc)
            raise BotoClientError(
                f"{self.service_name}.{operation} failed with {code}: {exc}",
                code=code,
                operation=operation,
            ) from exc
        except BotoCoreError as exc:
            # Connection refused, endpoint unreachable, read timeout, ...
            raise BotoClientError(
                f"{self.service_name}.{operation} failed: {exc}", operation=operation
            ) from exc
