from __future__ import annotations

from pathlib import Path

import pytest

from video_media_catalog.temporal.catalog_execution_worker import (
    assert_p3_runtime_enabled,
    worker_registration,
)
from video_media_catalog.temporal.names import (
    CATALOG_EXECUTION_ALIYUN_TASK_QUEUE,
    CATALOG_EXECUTION_AWS_TASK_QUEUE,
    CATALOG_EXECUTION_WORKFLOW_TASK_QUEUE,
)
from video_media_catalog.temporal.workflows.catalog_execution import (
    CatalogExecutionWorkflowV1,
)


def test_worker_roles_register_stable_least_privilege_queues() -> None:
    orchestrator = worker_registration("orchestrator")
    aws = worker_registration("aws-emr")
    aliyun = worker_registration("aliyun-emr")

    assert orchestrator["task_queue"] == CATALOG_EXECUTION_WORKFLOW_TASK_QUEUE
    assert orchestrator["workflows"] == [CatalogExecutionWorkflowV1]
    assert aws["task_queue"] == CATALOG_EXECUTION_AWS_TASK_QUEUE
    assert aliyun["task_queue"] == CATALOG_EXECUTION_ALIYUN_TASK_QUEUE
    assert aws["workflows"] == aliyun["workflows"] == []
    assert {
        activity.__temporal_activity_definition.name for activity in aws["activities"]
    } == {
        "SubmitCatalogSparkV1",
        "ObserveCatalogSparkV1",
        "CancelCatalogSparkV1",
    }


def test_worker_startup_remains_disabled_without_activation_receipt() -> None:
    with pytest.raises(RuntimeError, match="production activation remains disabled"):
        assert_p3_runtime_enabled()


def test_catalog_bridge_has_no_cross_repository_database_dependency() -> None:
    root = Path(__file__).parents[1] / "src/video_media_catalog"
    sources = "\n".join(
        path.read_text()
        for path in (
            root / "temporal/catalog_execution_worker.py",
            root / "temporal/activities/catalog_execution.py",
            root / "temporal/workflows/catalog_execution.py",
        )
    )

    for forbidden in (
        "psycopg",
        "sqlalchemy",
        "jooq",
        "platform_database",
        "catalog_database",
    ):
        assert forbidden not in sources.casefold()
