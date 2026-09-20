"""Production Spark stages for the owner-only research Silver v2 chain."""

from __future__ import annotations

import argparse
import os
import re
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, deterministic_key
from video_media_catalog.commit import SNAPSHOT_SET_MEDIA_TYPE
from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_ingest import (
    CommunityIngestCommit,
    CommunityIngestRun,
    IngestRunKind,
)
from video_media_catalog.community_snapshot import (
    CONTROL_MAX_BYTES,
    MAX_COMMITTED_RUNS,
    SILVER_SNAPSHOT_MEDIA_TYPE,
    CommunitySilverSnapshotSet,
    build_community_silver_snapshot_set,
)
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.constants import CURATED_TABLE_KEYS
from video_media_catalog.iceberg import (
    TABLE_COLUMNS as V1_TABLE_COLUMNS,
)
from video_media_catalog.iceberg import (
    CatalogConfig,
    MediaCatalogTables,
)
from video_media_catalog.identity_spark import (
    build_identity_resolution_dataframes,
)
from video_media_catalog.models import Checksum, ObjectRef, SnapshotSet
from video_media_catalog.object_store import (
    BoundedObjectStore,
    RuntimeObjectStore,
)
from video_media_catalog.v1_migration import build_v1_key_migration
from video_media_catalog.v2_contracts import (
    require_rfc3339,
    require_sha256,
)

MAX_URI_LENGTH = 2_048
MAX_OPTION_LENGTH = 4_096
MAX_SHUFFLE_PARTITIONS = 100_000
SOURCE_DATA_TABLES = frozenset(
    {
        "community_source_record",
        "community_field_assertion",
        "community_identifier_assertion",
        "community_relationship_assertion",
        "community_entity_type_assertion",
    }
)


def _add_control_object_args(
    parser: argparse.ArgumentParser,
    prefix: str,
) -> None:
    dashed = prefix.replace("_", "-")
    parser.add_argument(f"--{dashed}-uri", required=True)
    parser.add_argument(f"--{dashed}-hash", required=True)
    parser.add_argument(f"--{dashed}-size", required=True, type=int)
    parser.add_argument(f"--{dashed}-version", default="")
    parser.add_argument(f"--{dashed}-etag", default="")


def _add_v1_namespace_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--v1-namespace",
        default=os.environ.get("MEDIA_CATALOG_V1_NAMESPACE", "media_catalog"),
        help="Glue/Iceberg namespace for pinned v1 curated tables",
    )


def _add_catalog_args(
    parser: argparse.ArgumentParser,
    *,
    app_name: str,
) -> None:
    parser.add_argument(
        "--catalog-name",
        default=os.environ.get("MEDIA_CATALOG_CATALOG_NAME", "media"),
    )
    parser.add_argument(
        "--namespace",
        default=os.environ.get(
            "MEDIA_CATALOG_NAMESPACE",
            "video_media_catalog",
        ),
    )
    parser.add_argument(
        "--catalog-type",
        choices=("hadoop", "glue"),
        default=os.environ.get("MEDIA_CATALOG_CATALOG_TYPE", "glue"),
    )
    parser.add_argument(
        "--warehouse",
        default=os.environ.get("MEDIA_CATALOG_WAREHOUSE_URI"),
    )
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument(
        "--s3-path-style-access",
        action="store_true",
        default=os.environ.get("S3_PATH_STYLE", "").lower() in {"1", "true", "yes"},
    )
    parser.add_argument("--master")
    parser.add_argument("--app-name", default=app_name)
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--spark-packages")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-research-silver",
        description=(
            "Run snapshot-pinned research Silver migration, identity, and "
            "snapshot publication stages."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    migration = commands.add_parser(
        "migrate-v1",
        help="migrate published v1 keys from an immutable SnapshotSet",
    )
    _add_control_object_args(migration, "v1_snapshot")
    migration.add_argument("--committed-at", required=True)
    _add_v1_namespace_arg(migration)
    _add_catalog_args(
        migration,
        app_name="media-catalog-research-v1-migration",
    )
    migration.set_defaults(stage_runner=_run_v1_migration)

    identity = commands.add_parser(
        "resolve-identity",
        help="resolve explicitly selected committed source runs",
    )
    _add_control_object_args(identity, "silver_snapshot")
    _add_control_object_args(identity, "v1_snapshot")
    identity.add_argument(
        "--source-run-id",
        dest="source_run_ids",
        action="append",
        required=True,
    )
    identity.add_argument("--image-digest", required=True)
    identity.add_argument("--config-digest", required=True)
    identity.add_argument("--started-at", required=True)
    identity.add_argument("--committed-at", required=True)
    _add_v1_namespace_arg(identity)
    _add_catalog_args(
        identity,
        app_name="media-catalog-research-identity",
    )
    identity.set_defaults(stage_runner=_run_identity)

    publication = commands.add_parser(
        "publish-snapshot",
        help="publish a verified immutable Silver snapshot set for Gold",
    )
    publication.add_argument(
        "--run-id",
        dest="run_ids",
        action="append",
        required=True,
    )
    publication.add_argument("--snapshot-uri", required=True)
    publication.add_argument("--created-at", required=True)
    _add_catalog_args(
        publication,
        app_name="media-catalog-research-snapshot-publication",
    )
    publication.set_defaults(stage_runner=_run_snapshot_publication)
    return parser


def _bounded(
    value: str | None,
    *,
    label: str,
    max_length: int,
    required: bool = False,
) -> str | None:
    if value is None:
        if required:
            raise ValueError(f"{label} is required")
        return None
    normalized = value.strip()
    if required and not normalized:
        raise ValueError(f"{label} is required")
    if len(normalized) > max_length:
        raise ValueError(f"{label} exceeds {max_length} characters")
    return normalized


def _validate_object_uri(uri: str, *, label: str) -> str:
    normalized = _bounded(
        uri,
        label=label,
        max_length=MAX_URI_LENGTH,
        required=True,
    )
    assert normalized is not None
    parsed = urlsplit(normalized)
    if (
        parsed.scheme not in {"file", "s3"}
        or parsed.query
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError(
            f"{label} must use file:// or s3:// without credentials, query, or fragment"
        )
    if parsed.scheme == "s3" and (not parsed.netloc or not parsed.path.lstrip("/")):
        raise ValueError(f"{label} must identify one S3 object")
    if parsed.scheme == "file" and not parsed.path:
        raise ValueError(f"{label} must identify one local object")
    return normalized


def _control_object_ref(
    parsed: argparse.Namespace,
    prefix: str,
    *,
    media_type: str,
) -> ObjectRef:
    uri = _validate_object_uri(
        str(getattr(parsed, f"{prefix}_uri")),
        label=f"{prefix} URI",
    )
    digest = str(getattr(parsed, f"{prefix}_hash")).strip()
    match = re.fullmatch(
        r"(?:sha256:hex:|sha256:)?([0-9a-fA-F]{64})",
        digest,
    )
    if match is None:
        raise ValueError(f"{prefix} hash must contain 64 SHA-256 hex digits")
    size = int(getattr(parsed, f"{prefix}_size"))
    if not 0 < size <= CONTROL_MAX_BYTES:
        raise ValueError(f"{prefix} size must be between 1 byte and 16 MiB")
    version = _bounded(
        str(getattr(parsed, f"{prefix}_version")),
        label=f"{prefix} version",
        max_length=1_024,
    )
    etag = _bounded(
        str(getattr(parsed, f"{prefix}_etag")).strip('"'),
        label=f"{prefix} ETag",
        max_length=1_024,
    )
    version = version or None
    etag = etag or None
    scheme = urlsplit(uri).scheme
    if scheme == "s3" and (version is None or etag is None):
        raise ValueError(f"{prefix} S3 object requires version and ETag")
    if scheme == "file" and (version is not None or etag is not None):
        raise ValueError(f"{prefix} file object cannot declare version or ETag")
    return ObjectRef(
        uri=uri,
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=match.group(1).lower()),
        size_bytes=size,
        etag=etag,
        object_version=version,
    )


def _normalize_run_ids(
    values: Sequence[str],
    *,
    label: str,
) -> tuple[str, ...]:
    if not values:
        raise ValueError(f"{label} requires at least one run ID")
    if len(values) > MAX_COMMITTED_RUNS:
        raise ValueError(f"{label} supports at most {MAX_COMMITTED_RUNS} run IDs")
    normalized = tuple(require_sha256(value, label="run_id") for value in values)
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{label} contains duplicate run IDs")
    return tuple(sorted(normalized))


def _v1_catalog_config(parsed: argparse.Namespace) -> CatalogConfig:
    base = _catalog_config(parsed)
    v1_namespace = _bounded(
        parsed.v1_namespace,
        label="v1-namespace",
        max_length=128,
        required=True,
    )
    assert v1_namespace is not None
    return CatalogConfig(
        catalog_name=base.catalog_name,
        namespace=v1_namespace,
        warehouse=base.warehouse,
        catalog_type=base.catalog_type,
        aws_region=base.aws_region,
        s3_endpoint=base.s3_endpoint,
        s3_path_style_access=base.s3_path_style_access,
    )


def _catalog_config(parsed: argparse.Namespace) -> CatalogConfig:
    catalog_name = _bounded(
        parsed.catalog_name,
        label="catalog-name",
        max_length=128,
        required=True,
    )
    namespace = _bounded(
        parsed.namespace,
        label="namespace",
        max_length=128,
        required=True,
    )
    warehouse = _bounded(
        parsed.warehouse,
        label="warehouse",
        max_length=MAX_URI_LENGTH,
        required=True,
    )
    assert catalog_name is not None
    assert namespace is not None
    assert warehouse is not None
    if urlsplit(warehouse).scheme not in {"file", "s3", "s3a"}:
        raise ValueError("warehouse must use file://, s3://, or s3a://")
    _bounded(
        parsed.aws_region,
        label="aws-region",
        max_length=128,
    )
    endpoint = _bounded(
        parsed.s3_endpoint,
        label="s3-endpoint",
        max_length=MAX_URI_LENGTH,
    )
    if endpoint is not None and urlsplit(endpoint).scheme not in {"http", "https"}:
        raise ValueError("s3-endpoint must use http:// or https://")
    _bounded(parsed.master, label="master", max_length=256)
    _bounded(parsed.app_name, label="app-name", max_length=256, required=True)
    _bounded(
        parsed.spark_packages,
        label="spark-packages",
        max_length=MAX_OPTION_LENGTH,
    )
    if parsed.shuffle_partitions is not None and not (
        1 <= parsed.shuffle_partitions <= MAX_SHUFFLE_PARTITIONS
    ):
        raise ValueError(
            f"shuffle-partitions must be between 1 and {MAX_SHUFFLE_PARTITIONS}"
        )
    return CatalogConfig(
        catalog_name=catalog_name,
        namespace=namespace,
        warehouse=warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
    )


def _spark_session(
    parsed: argparse.Namespace,
    config: CatalogConfig,
) -> Any:
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(parsed.app_name)
    if parsed.master:
        builder = builder.master(parsed.master)
    builder = config.configure_builder(builder)
    if parsed.shuffle_partitions is not None:
        builder = builder.config(
            "spark.sql.shuffle.partitions",
            str(parsed.shuffle_partitions),
        )
    if parsed.spark_packages:
        builder = builder.config("spark.jars.packages", parsed.spark_packages)
    return builder.getOrCreate()


def _object_store(
    parsed: argparse.Namespace,
    *,
    local_only: bool,
) -> BoundedObjectStore:
    return BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=object() if local_only else None,
    )


def _read_model[T](
    store: RuntimeObjectStore,
    reference: ObjectRef,
    model: type[T],
) -> T:
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="research-silver-control-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "object.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        return model.model_validate_json(materialized.path.read_bytes())


def _load_v1_tables(
    spark: Any,
    *,
    config: CatalogConfig,
    snapshot_set: SnapshotSet,
) -> dict[str, Any]:
    catalog = MediaCatalogTables(spark, config)
    declared = {
        table.table_name.rsplit(".", 1)[-1]: table for table in snapshot_set.tables
    }
    if set(declared) != set(CURATED_TABLE_KEYS):
        raise ValueError("v1 SnapshotSet must contain all six curated tables")
    frames: dict[str, Any] = {}
    for table in V1_TABLE_COLUMNS:
        snapshot = declared[table]
        expected_name = catalog.table_name(table)
        if snapshot.table_name != expected_name:
            raise ValueError(
                f"v1 snapshot table {snapshot.table_name!r} does not match "
                f"configured table {expected_name!r}"
            )
        frames[table] = (
            spark.table(expected_name).limit(0)
            if snapshot.snapshot_id is None
            else spark.read.format("iceberg")
            .option("snapshot-id", str(snapshot.snapshot_id))
            .load(expected_name)
        )
    return frames


def _exact_control_rows(
    spark: Any,
    *,
    table_name: str,
    snapshot_id: int,
    run_ids: tuple[str, ...],
    json_column: str,
) -> list[Any]:
    from pyspark.sql import functions as F

    return (
        spark.read.format("iceberg")
        .option("snapshot-id", str(snapshot_id))
        .load(table_name)
        .where(F.col("run_id").isin(*run_ids))
        .select("run_id", json_column)
        .collect()
    )


def _models_by_run[T](
    rows: Sequence[Any],
    *,
    requested_run_ids: tuple[str, ...],
    json_column: str,
    model: type[T],
    label: str,
) -> dict[str, T]:
    values: dict[str, T] = {}
    for row in rows:
        run_id = str(row["run_id"])
        if run_id in values:
            raise RuntimeError(f"{label} contains duplicate run {run_id}")
        value = model.model_validate_json(row[json_column])
        if value.run_id != run_id:
            raise RuntimeError(f"{label} JSON does not match row run_id")
        values[run_id] = value
    missing = sorted(set(requested_run_ids) - set(values))
    if missing:
        raise ValueError(f"{label} is missing requested runs: {', '.join(missing)}")
    return values


def _load_run_state(
    spark: Any,
    *,
    tables: CommunityCatalogTables,
    run_snapshot_id: int,
    commit_snapshot_id: int,
    run_ids: tuple[str, ...],
) -> tuple[
    dict[str, CommunityIngestRun],
    dict[str, CommunityIngestCommit],
]:
    run_rows = _exact_control_rows(
        spark,
        table_name=tables.table_name("community_ingest_run"),
        snapshot_id=run_snapshot_id,
        run_ids=run_ids,
        json_column="manifest_json",
    )
    commit_rows = _exact_control_rows(
        spark,
        table_name=tables.table_name("community_ingest_commit"),
        snapshot_id=commit_snapshot_id,
        run_ids=run_ids,
        json_column="commit_json",
    )
    runs = _models_by_run(
        run_rows,
        requested_run_ids=run_ids,
        json_column="manifest_json",
        model=CommunityIngestRun,
        label="pinned ingest-run snapshot",
    )
    commits = _models_by_run(
        commit_rows,
        requested_run_ids=run_ids,
        json_column="commit_json",
        model=CommunityIngestCommit,
        label="pinned commit snapshot",
    )
    for run_id in run_ids:
        if runs[run_id].expected_counts != commits[run_id].table_counts:
            raise RuntimeError(f"run {run_id} manifest counts differ from its commit")
    return runs, commits


def _verify_data_counts(
    spark: Any,
    *,
    tables: CommunityCatalogTables,
    run_ids: tuple[str, ...],
    commits: dict[str, CommunityIngestCommit],
    data_snapshot_ids: dict[str, int | None],
) -> None:
    from pyspark.sql import functions as F

    for table in DATA_TABLE_COLUMNS:
        snapshot_id = data_snapshot_ids[table]
        expected = {run_id: commits[run_id].table_counts[table] for run_id in run_ids}
        if snapshot_id is None:
            if any(expected.values()):
                raise RuntimeError(
                    f"{table} has committed rows but no containing snapshot"
                )
            continue
        rows = (
            spark.read.format("iceberg")
            .option("snapshot-id", str(snapshot_id))
            .load(tables.table_name(table))
            .where(F.col("run_id").isin(*run_ids))
            .groupBy("run_id")
            .count()
            .collect()
        )
        actual = {str(row["run_id"]): int(row["count"]) for row in rows}
        for run_id, expected_count in expected.items():
            if actual.get(run_id, 0) != expected_count:
                raise RuntimeError(
                    f"{table} count for run {run_id} differs from its commit"
                )


def _selected_silver_frames(
    spark: Any,
    *,
    tables: CommunityCatalogTables,
    snapshot: CommunitySilverSnapshotSet,
    source_run_ids: tuple[str, ...],
) -> dict[str, Any]:
    visible = tables.visible_dataframes(
        data_snapshot_ids=snapshot.data_snapshot_ids,
        commit_snapshot_id=snapshot.commit_snapshot_id,
    )
    selected_runs = spark.createDataFrame(
        [(run_id,) for run_id in snapshot.committed_run_ids],
        "run_id STRING",
    )
    source_runs = spark.createDataFrame(
        [(run_id,) for run_id in source_run_ids],
        "run_id STRING",
    )
    result = {
        table: frame.join(selected_runs, "run_id", "inner")
        for table, frame in visible.items()
    }
    for table in SOURCE_DATA_TABLES:
        result[table] = result[table].join(source_runs, "run_id", "inner")
    return result


def _run_v1_migration(parsed: argparse.Namespace) -> dict[str, Any]:
    committed_at = require_rfc3339(parsed.committed_at, label="committed-at")
    reference = _control_object_ref(
        parsed,
        "v1_snapshot",
        media_type=SNAPSHOT_SET_MEDIA_TYPE,
    )
    store = _object_store(
        parsed,
        local_only=urlsplit(reference.uri).scheme == "file",
    )
    snapshot_set = _read_model(store, reference, SnapshotSet)
    config = _catalog_config(parsed)
    spark = _spark_session(parsed, config)
    frames: dict[str, Any] | None = None
    try:
        v1_tables = _load_v1_tables(
            spark,
            config=_v1_catalog_config(parsed),
            snapshot_set=snapshot_set,
        )
        run, frames = build_v1_key_migration(
            spark,
            snapshot_set=snapshot_set,
            v1_tables=v1_tables,
            snapshot_object_ref=reference,
        )
        commit = CommunityCatalogTables(spark, config).stage_and_commit(
            run=run,
            dataframes=frames,
            committed_at=committed_at,
        )
        return {
            "context": "research",
            "stage": "v1-migration",
            "runId": run.run_id,
            "commitKey": commit.commit_key,
            "v1SnapshotSetId": snapshot_set.snapshot_set_id,
            "tableCounts": commit.table_counts,
            "tableSnapshotIds": commit.table_snapshot_ids,
        }
    finally:
        if frames is not None:
            for frame in frames.values():
                frame.unpersist()
        spark.stop()


def _run_identity(parsed: argparse.Namespace) -> dict[str, Any]:
    source_run_ids = _normalize_run_ids(
        parsed.source_run_ids,
        label="resolve-identity",
    )
    started_at = require_rfc3339(parsed.started_at, label="started-at")
    committed_at = require_rfc3339(parsed.committed_at, label="committed-at")
    image_digest = require_sha256(
        parsed.image_digest,
        label="image-digest",
    )
    config_digest = require_sha256(
        parsed.config_digest,
        label="config-digest",
    )
    silver_ref = _control_object_ref(
        parsed,
        "silver_snapshot",
        media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
    )
    v1_ref = _control_object_ref(
        parsed,
        "v1_snapshot",
        media_type=SNAPSHOT_SET_MEDIA_TYPE,
    )
    local_inputs = all(
        urlsplit(reference.uri).scheme == "file" for reference in (silver_ref, v1_ref)
    )
    store = _object_store(parsed, local_only=local_inputs)
    silver_snapshot = _read_model(
        store,
        silver_ref,
        CommunitySilverSnapshotSet,
    )
    v1_snapshot = _read_model(store, v1_ref, SnapshotSet)
    missing_sources = sorted(
        set(source_run_ids) - set(silver_snapshot.committed_run_ids)
    )
    if missing_sources:
        raise ValueError(
            "source runs are absent from the pinned Silver snapshot: "
            + ", ".join(missing_sources)
        )

    config = _catalog_config(parsed)
    spark = _spark_session(parsed, config)
    frames: dict[str, Any] | None = None
    try:
        tables = CommunityCatalogTables(spark, config)
        runs, _ = _load_run_state(
            spark,
            tables=tables,
            run_snapshot_id=silver_snapshot.run_snapshot_id,
            commit_snapshot_id=silver_snapshot.commit_snapshot_id,
            run_ids=silver_snapshot.committed_run_ids,
        )
        wrong_kind = [
            run_id
            for run_id in source_run_ids
            if runs[run_id].run_kind != IngestRunKind.SOURCE_ASSERTIONS
        ]
        if wrong_kind:
            raise ValueError(
                "resolve-identity accepts only committed SOURCE_ASSERTIONS runs: "
                + ", ".join(wrong_kind)
            )
        visible = _selected_silver_frames(
            spark,
            tables=tables,
            snapshot=silver_snapshot,
            source_run_ids=source_run_ids,
        )
        v1_tables = _load_v1_tables(
            spark,
            config=_v1_catalog_config(parsed),
            snapshot_set=v1_snapshot,
        )
        pinned_inputs = {
            "silverSnapshot": {
                "object": silver_ref.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                ),
                "snapshotSetId": silver_snapshot.snapshot_set_id,
            },
            "sourceRunIds": source_run_ids,
            "v1Snapshot": {
                "object": v1_ref.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                ),
                "snapshotSetId": v1_snapshot.snapshot_set_id,
            },
        }
        registry = build_community_registry()
        input_id = deterministic_key(
            "community-identity-resolution-input-v2",
            pinned_inputs,
        )
        run, frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=visible,
            v1_external_identifiers=v1_tables["catalog_external_identifier"],
            v1_entities=v1_tables["catalog_entity"],
            input_id=input_id,
            image_digest=image_digest,
            config_digest=config_digest,
            started_at=started_at,
            registry=registry,
            pinned_inputs=pinned_inputs,
        )
        commit = tables.stage_and_commit(
            run=run,
            dataframes=frames,
            committed_at=committed_at,
        )
        return {
            "context": "research",
            "stage": "identity-resolution",
            "runId": run.run_id,
            "commitKey": commit.commit_key,
            "registryDigest": registry.digest,
            "sourceRunIds": source_run_ids,
            "tableCounts": commit.table_counts,
            "tableSnapshotIds": commit.table_snapshot_ids,
        }
    finally:
        if frames is not None:
            for frame in frames.values():
                frame.unpersist()
        spark.stop()


def _require_immutable_snapshot_output(reference: ObjectRef) -> None:
    if reference.size_bytes <= 0 or not reference.checksum.value:
        raise RuntimeError("snapshot publication returned incomplete hash or size")
    if urlsplit(reference.uri).scheme == "s3" and (
        reference.object_version is None or reference.etag is None
    ):
        raise RuntimeError(
            "S3 snapshot publication requires bucket versioning and an ETag"
        )


def _run_snapshot_publication(
    parsed: argparse.Namespace,
) -> dict[str, Any]:
    run_ids = _normalize_run_ids(
        parsed.run_ids,
        label="publish-snapshot",
    )
    created_at = require_rfc3339(parsed.created_at, label="created-at")
    destination_uri = _validate_object_uri(
        parsed.snapshot_uri,
        label="snapshot URI",
    )
    config = _catalog_config(parsed)
    spark = _spark_session(parsed, config)
    try:
        tables = CommunityCatalogTables(spark, config)
        commit_snapshot_id = tables.latest_snapshot_id("community_ingest_commit")
        if commit_snapshot_id is None:
            raise ValueError("community_ingest_commit has no committed snapshot")
        run_snapshot_id = tables.latest_snapshot_id("community_ingest_run")
        if run_snapshot_id is None:
            raise ValueError("community_ingest_run has no committed snapshot")
        data_snapshot_ids = {
            table: tables.latest_snapshot_id(table) for table in DATA_TABLE_COLUMNS
        }
        _, commits = _load_run_state(
            spark,
            tables=tables,
            run_snapshot_id=run_snapshot_id,
            commit_snapshot_id=commit_snapshot_id,
            run_ids=run_ids,
        )
        _verify_data_counts(
            spark,
            tables=tables,
            run_ids=run_ids,
            commits=commits,
            data_snapshot_ids=data_snapshot_ids,
        )
        snapshot = build_community_silver_snapshot_set(
            committed_run_ids=run_ids,
            run_snapshot_id=run_snapshot_id,
            commit_snapshot_id=commit_snapshot_id,
            data_snapshot_ids=data_snapshot_ids,
            created_at=created_at,
        )
        store = _object_store(
            parsed,
            local_only=urlsplit(destination_uri).scheme == "file",
        )
        result = store.upload_bytes(
            snapshot.json_bytes(),
            destination_uri,
            media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=CONTROL_MAX_BYTES,
        )
        reference = result.object_ref
        _require_immutable_snapshot_output(reference)
        store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
        return {
            "context": "research",
            "stage": "silver-snapshot-publication",
            "snapshotSetId": snapshot.snapshot_set_id,
            "committedRunIds": snapshot.committed_run_ids,
            "runSnapshotId": snapshot.run_snapshot_id,
            "commitSnapshotId": snapshot.commit_snapshot_id,
            "dataSnapshotIds": snapshot.data_snapshot_ids,
            "silverSnapshot": reference.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "reused": result.reused,
        }
    finally:
        spark.stop()


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    return parsed.stage_runner(parsed)


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
