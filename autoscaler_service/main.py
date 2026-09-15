"""Entry point: boots the scaler and serves the HTTP API on port 8000.

Startup order matters. ``docker compose up`` starts this container *before* the
operator runs ``seed.py``, so the bucket, queue and table may not exist yet. The
API therefore comes up immediately (``/health`` reports ``starting``) while a
background thread keeps probing S3 for the manifest and rules; once both load, the
worker threads start and ``/rules`` and ``/clusters`` switch from 503 to 200.
"""
from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException

from autoscaler_service.aws_clients import BotoClientError, DynamoDbClient, S3Client, SqsClient
from autoscaler_service.config import Settings
from autoscaler_service.debug_log import DebugLogger
from autoscaler_service.engine import DecisionEngine, Scaler
from autoscaler_service.parsers import SparkLogParser

logger = logging.getLogger("autoscaler")


def configure_logging(level: str, debug_mode: bool = False) -> None:
    """Debug mode always logs at DEBUG; otherwise LOG_LEVEL applies."""
    logging.basicConfig(
        level=logging.DEBUG if debug_mode else getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(threadName)s %(name)s: %(message)s",
    )
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


@dataclass
class AppState:
    """Mutable service state shared between the bootstrap thread and the API."""

    settings: Settings
    s3: S3Client
    sqs: SqsClient
    dynamodb: DynamoDbClient
    rules: list[dict[str, Any]] | None = None  # the validated `rules` list from rules.yaml
    manifest: list[dict[str, Any]] | None = None  # the validated `clusters` list from clusters.json
    scaler: Scaler | None = None
    bootstrap_error: str | None = None
    stop_event: threading.Event = field(default_factory=threading.Event)

    @property
    def config_loaded(self) -> bool:
        return self.rules is not None and self.manifest is not None


def build_state(settings: Settings) -> AppState:
    return AppState(
        settings=settings,
        s3=S3Client(settings),
        sqs=SqsClient(settings),
        dynamodb=DynamoDbClient(settings),
    )


def load_config_from_s3(s3: S3Client, settings: Settings) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Fetch rules.yaml and clusters.json and return their ``rules`` / ``clusters`` lists.

    Only the top-level shape is checked here; the entries themselves are validated
    (and any problems logged) by ``DecisionEngine`` and ``Scaler`` when they are built.
    """
    rules_doc = s3.get_yaml(settings.rules_key)
    if not isinstance(rules_doc, dict) or "rules" not in rules_doc:
        raise ValueError(f"{settings.rules_key} must be a mapping with a 'rules' key")
    manifest_doc = s3.get_json(settings.manifest_key)
    if not isinstance(manifest_doc, dict) or "clusters" not in manifest_doc:
        raise ValueError(f"{settings.manifest_key} must be a mapping with a 'clusters' key")
    return rules_doc["rules"], manifest_doc["clusters"]


def bootstrap(state: AppState) -> None:
    """Wait for the seed data, then start the scaler. Runs in its own thread."""
    settings = state.settings
    while not state.stop_event.is_set():
        try:
            rules, manifest = load_config_from_s3(state.s3, settings)
            debug_logger = DebugLogger(settings.debug_log_dir) if settings.debug_mode else None
            # Building these validates the rules and manifest (problems are logged).
            engine = DecisionEngine(rules, trace=debug_logger.log_decision_trace if debug_logger else None)
            scaler = Scaler(
                settings,
                sqs=state.sqs,
                s3=state.s3,
                dynamodb=state.dynamodb,
                manifest=manifest,
                engine=engine,
                parser=SparkLogParser(),
                debug_logger=debug_logger,
            )
        except BotoClientError as exc:
            logger.warning(
                f"config not available yet ({exc}); has seed.py run? retrying in {settings.startup_retry_seconds:.0f}s"
            )
            state.stop_event.wait(settings.startup_retry_seconds)
            continue
        except (ValueError, TypeError, yaml.YAMLError) as exc:  # JSONDecodeError is a ValueError
            # A malformed rules/manifest file is an operator error, not a transient
            # one: surface it on /health and stop trying rather than spin forever.
            state.bootstrap_error = f"invalid configuration in S3: {exc}"
            logger.error(state.bootstrap_error)
            return

        state.rules = rules
        state.manifest = manifest
        logger.info(f"loaded {len(rules)} rule(s) and {len(manifest)} cluster(s) from s3://{settings.s3_bucket}")
        scaler.start()
        state.scaler = scaler
        return


def create_app(settings: Settings | None = None, state: AppState | None = None, *, start_bootstrap: bool = True) -> FastAPI:
    """Build the FastAPI app. Tests pass a pre-populated ``state`` and
    ``start_bootstrap=False`` to exercise the endpoints without AWS."""
    settings = settings or Settings.from_env()
    state = state or build_state(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        thread: threading.Thread | None = None
        if start_bootstrap:
            thread = threading.Thread(target=bootstrap, args=(state,), name="bootstrap", daemon=True)
            thread.start()
        try:
            yield
        finally:
            state.stop_event.set()
            if state.scaler is not None:
                state.scaler.stop(timeout=5)
            if thread is not None:
                thread.join(timeout=5)

    app = FastAPI(title="Streaming Job Autoscaler", version="1.0.0", lifespan=lifespan)
    app.state.service = state

    @app.get("/health")
    def health() -> dict:
        if state.bootstrap_error:
            raise HTTPException(status_code=503, detail={"status": "error", "error": state.bootstrap_error})
        alive = state.scaler.workers_alive() if state.scaler is not None else 0
        if not state.config_loaded:
            status = "starting"
        elif alive < settings.num_of_workers:
            status = "degraded"  # a worker thread died; the run cannot finish on its own
        else:
            status = "ok"
        return {"status": status, "workers": {"alive": alive, "configured": settings.num_of_workers}}

    @app.get("/rules")
    def rules() -> dict:
        if state.rules is None:
            raise HTTPException(status_code=503, detail="rules not loaded yet")
        return {"rules": state.rules}

    @app.get("/clusters")
    def clusters() -> dict:
        if state.manifest is None:
            raise HTTPException(status_code=503, detail="clusters not loaded yet")
        if state.scaler is not None:
            return {"clusters": state.scaler.status()["clusters"]}
        return {"clusters": state.manifest}

    return app


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.debug_mode)
    logger.info(
        f"starting autoscaler: endpoint={settings.aws_endpoint_url} bucket={settings.s3_bucket} "
        f"queue={settings.sqs_queue_name} table={settings.dynamodb_table} workers={settings.num_of_workers} "
        f"debug_mode={settings.debug_mode}"
    )
    uvicorn.run(
        create_app(settings),
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
        access_log=False,
    )


if __name__ == "__main__":
    main()
