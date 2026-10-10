"""Submit one packaged PySpark entry point to EMR Serverless and wait for it."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import signal
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from typing import Any

_RUNNABLE_APPLICATION_STATES = {"CREATED", "STARTED", "STOPPED"}
_SUCCESS_STATES = {"SUCCESS"}
_FAILURE_STATES = {"FAILED", "CANCELLED"}
_TERMINAL_STATES = _SUCCESS_STATES | _FAILURE_STATES
_CLIENT_TOKEN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _entry_point_arguments(parsed: argparse.Namespace) -> list[str]:
    arguments = list(parsed.entry_point_arguments)
    if arguments[:1] == ["--"]:
        arguments.pop(0)
    return arguments


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-emr-submit",
        description=(
            "Resolve one EMR Serverless application by name, submit a packaged "
            "PySpark entry point, and mirror its terminal status."
        ),
    )
    parser.add_argument("--application-name", required=True)
    parser.add_argument("--execution-role-arn", required=True)
    parser.add_argument("--job-name", required=True)
    parser.add_argument("--client-token", required=True)
    parser.add_argument("--entry-point", required=True)
    parser.add_argument("--log-uri", required=True)
    parser.add_argument("--aws-region", required=True)
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--execution-timeout-minutes", type=int, default=720)
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--driver-cores", type=int, default=4)
    parser.add_argument("--driver-memory", default="16g")
    parser.add_argument("--driver-memory-overhead")
    parser.add_argument("--driver-disk", default="50G")
    parser.add_argument("--executor-cores", type=int, default=4)
    parser.add_argument("--executor-memory", default="24g")
    parser.add_argument("--executor-memory-overhead")
    parser.add_argument("--executor-disk", default="200G")
    parser.add_argument("--executor-instances", type=int, default=4)
    parser.add_argument("--shuffle-partitions", type=int, default=96)
    parser.add_argument(
        "--enable-speculation",
        action="store_true",
        help="relaunch bounded stragglers after most tasks in a stage complete",
    )
    parser.add_argument(
        "entry_point_arguments",
        nargs=argparse.REMAINDER,
        help="arguments after -- are forwarded to the packaged PySpark entry point",
    )
    return parser


def _validate(parsed: argparse.Namespace) -> None:
    if not _CLIENT_TOKEN.fullmatch(parsed.client_token):
        raise ValueError(
            "client-token must contain 1-64 letters, digits, '.', '_', or '-'"
        )
    if not parsed.application_name.strip():
        raise ValueError("application-name must be non-empty")
    if not parsed.job_name.strip() or len(parsed.job_name) > 64:
        raise ValueError("job-name must contain between 1 and 64 characters")
    if parsed.poll_seconds < 1:
        raise ValueError("poll-seconds must be positive")
    if parsed.execution_timeout_minutes < 1:
        raise ValueError("execution-timeout-minutes must be positive")
    if parsed.max_attempts < 1:
        raise ValueError("max-attempts must be positive")
    for name in ("driver_cores", "executor_cores", "executor_instances"):
        if getattr(parsed, name) < 1:
            raise ValueError(f"{name.replace('_', '-')} must be positive")
    if parsed.shuffle_partitions < 1:
        raise ValueError("shuffle-partitions must be positive")
    if not _entry_point_arguments(parsed):
        raise ValueError("at least one entry-point argument is required after --")


def _applications(client: Any) -> list[dict[str, Any]]:
    applications: list[dict[str, Any]] = []
    request: dict[str, Any] = {}
    while True:
        response = client.list_applications(**request)
        applications.extend(response.get("applications", []))
        token = response.get("nextToken")
        if not token:
            return applications
        request["nextToken"] = token


def _application_id(client: Any, name: str) -> str:
    applications = _applications(client)
    matches = [
        application
        for application in applications
        if application.get("name") == name
        and application.get("state") in _RUNNABLE_APPLICATION_STATES
    ]
    if len(matches) != 1:
        states = sorted(
            str(application.get("state"))
            for application in applications
            if application.get("name") == name
        )
        raise RuntimeError(
            f"expected exactly one runnable EMR Serverless application named "
            f"{name!r}; found {len(matches)} (states={states})"
        )
    return str(matches[0]["id"])


def _spark_submit_parameters(parsed: argparse.Namespace) -> str:
    python = "/opt/video-media-catalog/.venv/bin/python"
    speculation_enabled = bool(getattr(parsed, "enable_speculation", False))
    properties = {
        "spark.driver.cores": str(parsed.driver_cores),
        "spark.driver.memory": parsed.driver_memory,
        "spark.executor.cores": str(parsed.executor_cores),
        "spark.executor.memory": parsed.executor_memory,
        "spark.executor.instances": str(parsed.executor_instances),
        "spark.dynamicAllocation.enabled": "false",
        "spark.sql.shuffle.partitions": str(parsed.shuffle_partitions),
        "spark.default.parallelism": str(parsed.shuffle_partitions),
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "3",
        "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "64m",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64m",
        "spark.speculation": str(speculation_enabled).lower(),
        # Transient S3/Iceberg read failures can leave an executor's AWS SDK
        # connection pool shut down; pin retries away from that executor.
        "spark.excludeOnFailure.enabled": "true",
        "spark.excludeOnFailure.task.maxTaskAttemptsPerExecutor": "1",
        "spark.excludeOnFailure.task.maxTaskAttemptsPerNode": "2",
        "spark.excludeOnFailure.stage.maxFailedTasksPerExecutor": "2",
        "spark.excludeOnFailure.application.maxFailedTasksPerExecutor": "2",
        # A closed Iceberg client permanently poisons that executor. Kill it
        # immediately so EMR replaces it instead of re-admitting it after the
        # default one-hour exclusion timeout.
        "spark.excludeOnFailure.killExcludedExecutors": "true",
        "spark.excludeOnFailure.timeout": "24h",
        "spark.task.maxFailures": "8",
        # Gold field groupByKey can lose shuffle map outputs when an executor
        # GC/stalls past the heartbeat window; allow more stage retries while
        # network timeout absorbs the stall.
        "spark.stage.maxConsecutiveAttempts": "20",
        "spark.shuffle.io.maxRetries": "16",
        "spark.shuffle.io.retryWait": "15s",
        # Preserve cached RDD and shuffle blocks during graceful executor
        # retirement. Abrupt executor loss is covered by replicated Gold RDDs.
        "spark.decommission.enabled": "true",
        "spark.storage.decommission.enabled": "true",
        "spark.storage.decommission.rddBlocks.enabled": "true",
        "spark.storage.decommission.shuffleBlocks.enabled": "true",
        "spark.storage.replication.proactive": "true",
        "spark.scheduler.listenerbus.eventqueue.capacity": "200000",
        # Field/relation resolution can still host multi-tens-of-minutes reduce
        # tasks on skewed keys; keep HeartbeatReceiver above those stalls
        # (timeout == spark.network.timeout).
        "spark.rpc.askTimeout": "3600s",
        "spark.network.timeout": "3600s",
        "spark.executor.heartbeatInterval": "30s",
        "spark.shuffle.io.connectionTimeout": "3600s",
        "spark.executor.extraJavaOptions": (
            "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 "
            "-XX:+ParallelRefProcEnabled -XX:MaxGCPauseMillis=1000"
        ),
        "spark.emr-serverless.driver.disk": parsed.driver_disk,
        "spark.emr-serverless.executor.disk": parsed.executor_disk,
        "spark.emr-serverless.driver.disk.type": "SHUFFLE_OPTIMIZED",
        "spark.emr-serverless.executor.disk.type": "SHUFFLE_OPTIMIZED",
        "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON": python,
        "spark.emr-serverless.driverEnv.PYSPARK_PYTHON": python,
        "spark.executorEnv.PYSPARK_PYTHON": python,
    }
    if speculation_enabled:
        # A full local disk can leave already-assigned tasks alive even after
        # Spark excludes the executor for the stage. Relaunch only after 90%
        # completion and five minutes so normal skew is not duplicated.
        properties.update(
            {
                "spark.speculation.interval": "10s",
                "spark.speculation.multiplier": "4",
                "spark.speculation.quantile": "0.9",
                "spark.speculation.minTaskRuntime": "300s",
                "spark.speculation.efficiency.enabled": "false",
            }
        )
    if parsed.driver_memory_overhead:
        properties["spark.driver.memoryOverhead"] = parsed.driver_memory_overhead
    if parsed.executor_memory_overhead:
        properties["spark.executor.memoryOverhead"] = parsed.executor_memory_overhead
    arguments: list[str] = []
    for key, value in properties.items():
        arguments.extend(("--conf", f"{key}={value}"))
    return shlex.join(arguments)


def _emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")), flush=True)


def run(
    parsed: argparse.Namespace,
    *,
    client: Any | None = None,
    sleep: Callable[[float], None] = time.sleep,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
    resume_job_run_id: str | None = None,
) -> dict[str, Any]:
    _validate(parsed)
    if client is None:
        import boto3

        client = boto3.client("emr-serverless", region_name=parsed.aws_region)

    def _progress(event: dict[str, Any]) -> None:
        _emit(event)
        if progress_callback is not None:
            progress_callback(event)

    application_id = _application_id(client, parsed.application_name)
    request: dict[str, Any] = {
        "applicationId": application_id,
        "clientToken": parsed.client_token,
        "executionRoleArn": parsed.execution_role_arn,
        "executionTimeoutMinutes": parsed.execution_timeout_minutes,
        "jobDriver": {
            "sparkSubmit": {
                "entryPoint": parsed.entry_point,
                "entryPointArguments": _entry_point_arguments(parsed),
                "sparkSubmitParameters": _spark_submit_parameters(parsed),
            }
        },
        "name": parsed.job_name,
        "configurationOverrides": {
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": parsed.log_uri}
            }
        },
        "retryPolicy": {"maxAttempts": parsed.max_attempts},
    }

    job_run_id: str | None = None
    terminal = False
    try:
        if resume_job_run_id is None:
            started = client.start_job_run(**request)
            job_run_id = str(started["jobRunId"])
            _progress(
                {
                    "applicationId": application_id,
                    "event": "submitted",
                    "jobRunArn": started.get("arn"),
                    "jobRunId": job_run_id,
                }
            )
        else:
            job_run_id = resume_job_run_id
            _progress(
                {
                    "applicationId": application_id,
                    "event": "resumed",
                    "jobRunId": job_run_id,
                }
            )

        previous_state: str | None = None
        while True:
            if should_cancel is not None and should_cancel():
                raise RuntimeError(
                    f"EMR Serverless job {job_run_id} cancelled by caller"
                )
            response = client.get_job_run(
                applicationId=application_id,
                jobRunId=job_run_id,
            )
            job_run = response["jobRun"]
            state = str(job_run["state"])
            if state != previous_state:
                _progress(
                    {
                        "applicationId": application_id,
                        "event": "state",
                        "jobRunId": job_run_id,
                        "state": state,
                        "stateDetails": job_run.get("stateDetails"),
                    }
                )
                previous_state = state
            elif progress_callback is not None:
                # A job can remain SCHEDULED while EMR acquires capacity for
                # longer than the Temporal heartbeat timeout. Keep the
                # activity alive even when the EMR state has not changed.
                progress_callback(
                    {
                        "applicationId": application_id,
                        "event": "poll",
                        "jobRunId": job_run_id,
                        "state": state,
                        "stateDetails": job_run.get("stateDetails"),
                    }
                )
            if state in _TERMINAL_STATES:
                terminal = True
                if state in _SUCCESS_STATES:
                    return job_run
                raise RuntimeError(
                    f"EMR Serverless job {job_run_id} ended in {state}: "
                    f"{job_run.get('stateDetails') or 'no state details'}"
                )
            sleep(parsed.poll_seconds)
    finally:
        if job_run_id is not None and not terminal:
            with suppress(Exception):
                client.cancel_job_run(
                    applicationId=application_id,
                    jobRunId=job_run_id,
                )


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)

    def _terminate(_signum: int, _frame: Any) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, _terminate)
    run(parsed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
