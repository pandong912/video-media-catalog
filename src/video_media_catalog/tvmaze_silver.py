"""Project TVmaze connector records into validated Silver v2 rows."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from video_media_catalog.community_ingest import CommunityIngestRun
from video_media_catalog.connector import (
    ConnectorBatchManifest,
    ConnectorRecordEnvelope,
    ConnectorRecordSetManifest,
)
from video_media_catalog.source_registry import SourceRegistrySnapshot
from video_media_catalog.source_silver import (
    build_source_silver_dataframes,
    build_source_silver_rows,
)


def build_tvmaze_silver_rows(
    *,
    registry: SourceRegistrySnapshot,
    batch: ConnectorBatchManifest,
    record_set: ConnectorRecordSetManifest,
    envelopes: Iterable[ConnectorRecordEnvelope],
    max_records: int = 10_000,
) -> tuple[CommunityIngestRun, dict[str, list[dict[str, Any]]]]:
    """Build a bounded local run; production backfills use the Spark variant."""

    return build_source_silver_rows(
        registry=registry,
        batch=batch,
        record_set=record_set,
        envelopes=envelopes,
        max_records=max_records,
    )


def build_tvmaze_silver_dataframes(
    spark: Any,
    *,
    registry: SourceRegistrySnapshot,
    batch: ConnectorBatchManifest,
    record_set: ConnectorRecordSetManifest,
) -> tuple[CommunityIngestRun, dict[str, Any]]:
    """Distributed record-set projection; callers must unpersist returned frames."""

    return build_source_silver_dataframes(
        spark,
        registry=registry,
        batch=batch,
        record_set=record_set,
    )
