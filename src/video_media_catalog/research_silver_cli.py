"""Production Spark stages for the shared authenticated research Silver chain."""

from __future__ import annotations

import argparse
import os
import re
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, deterministic_key
from video_media_catalog.catalog_source_policy import load_catalog_source_policy
from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_ingest import (
    CommunityIngestCommit,
    CommunityIngestRun,
    IngestRunKind,
)
from video_media_catalog.community_snapshot import (
    CONTROL_MAX_BYTES,
    MAX_EPOCH_DELTA_RUNS,
    SILVER_EPOCH_MEDIA_TYPE,
    SILVER_SNAPSHOT_MEDIA_TYPE,
    CommunitySilverEpochManifest,
    CommunitySilverEpochReference,
    CommunitySilverManifest,
    CommunitySilverSnapshotSet,
    build_community_silver_epoch_manifest,
    build_community_silver_snapshot_set,
    community_silver_manifest_id,
    community_silver_table_mapping,
    parse_community_silver_manifest,
)
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    IDENTITY_TABLES,
    SOURCE_TABLES,
    require_identity_generation_id,
)
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.identity_spark import (
    IdentityResolutionConfig,
    build_identity_resolution_dataframes,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    RuntimeObjectStore,
)
from video_media_catalog.v2_contracts import (
    parse_rfc3339,
    require_rfc3339,
    require_sha256,
    require_slug,
)

MAX_URI_LENGTH = 2_048
MAX_OPTION_LENGTH = 4_096
MAX_SHUFFLE_PARTITIONS = 100_000
MAX_EXPLICIT_RUN_IDS = MAX_EPOCH_DELTA_RUNS
SOURCE_DATA_TABLES = SOURCE_TABLES


def _add_control_object_args(
    parser: argparse.ArgumentParser,
    prefix: str,
    *,
    required: bool = True,
) -> None:
    dashed = prefix.replace("_", "-")
    parser.add_argument(f"--{dashed}-uri", required=required)
    parser.add_argument(f"--{dashed}-hash", required=required)
    parser.add_argument(f"--{dashed}-size", required=required, type=int)
    parser.add_argument(f"--{dashed}-version", default="")
    parser.add_argument(f"--{dashed}-etag", default="")


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
    parser.add_argument(
        "--s3-credentials-provider",
        choices=("web-identity", "default"),
        default="web-identity",
    )
    parser.add_argument("--master")
    parser.add_argument("--app-name", default=app_name)
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--spark-packages")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-research-silver",
        description=(
            "Run source-pinned research Silver identity and snapshot "
            "publication stages."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    identity = commands.add_parser(
        "resolve-identity",
        help="resolve explicitly selected committed source runs",
    )
    _add_control_object_args(identity, "silver_snapshot")
    identity.add_argument(
        "--silver-snapshot-media-type",
        choices=(SILVER_SNAPSHOT_MEDIA_TYPE, SILVER_EPOCH_MEDIA_TYPE),
        default=SILVER_SNAPSHOT_MEDIA_TYPE,
    )
    identity.add_argument(
        "--source-run-id",
        dest="source_run_ids",
        action="append",
        required=True,
    )
    identity.add_argument("--identity-generation-id", required=True)
    identity.add_argument(
        "--identity-mode",
        required=True,
        choices=("full", "incremental"),
    )
    identity.add_argument("--image-digest", required=True)
    identity.add_argument("--config-digest", required=True)
    identity.add_argument("--source-policy-digest", default="")
    identity_defaults = IdentityResolutionConfig()
    identity.add_argument(
        "--identity-max-label-iterations",
        type=int,
        default=identity_defaults.max_exact_blocking_label_iterations,
    )
    identity.add_argument(
        "--identity-max-component-size",
        type=int,
        default=identity_defaults.max_exact_blocking_component_size,
    )
    identity.add_argument(
        "--identity-max-node-candidate-keys",
        type=int,
        default=identity_defaults.max_exact_blocking_node_candidate_keys,
    )
    identity.add_argument(
        "--identity-max-component-candidate-keys",
        type=int,
        default=identity_defaults.max_exact_blocking_component_candidate_keys,
    )
    identity.add_argument("--started-at", required=True)
    identity.add_argument("--committed-at", required=True)
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
    publication.add_argument("--identity-generation-id")
    publication.add_argument(
        "--initialize-identity-generation",
        action="store_true",
    )
    _add_catalog_args(
        publication,
        app_name="media-catalog-research-snapshot-publication",
    )
    publication.set_defaults(stage_runner=_run_snapshot_publication)

    epoch = commands.add_parser(
        "publish-epoch",
        help="publish a bounded epoch manifest over all pinned committed runs",
    )
    epoch.add_argument(
        "--delta-run-id",
        dest="delta_run_ids",
        action="append",
        default=[],
    )
    epoch.add_argument(
        "--epoch-uri",
        "--snapshot-uri",
        dest="epoch_uri",
        required=True,
    )
    epoch.add_argument(
        "--source-watermark",
        dest="source_watermarks",
        action="append",
        default=[],
        metavar="SOURCE_PRODUCT_ID=VALUE",
    )
    _add_control_object_args(epoch, "parent_epoch", required=False)
    epoch.add_argument("--created-at", required=True)
    epoch.add_argument("--identity-generation-id")
    _add_catalog_args(
        epoch,
        app_name="media-catalog-research-epoch-publication",
    )
    epoch.set_defaults(stage_runner=_run_epoch_publication)
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


def _optional_control_object_ref(
    parsed: argparse.Namespace,
    prefix: str,
    *,
    media_type: str,
) -> ObjectRef | None:
    values = (
        getattr(parsed, f"{prefix}_uri"),
        getattr(parsed, f"{prefix}_hash"),
        getattr(parsed, f"{prefix}_size"),
    )
    if all(value is None for value in values):
        return None
    if any(value is None for value in values):
        raise ValueError(
            f"{prefix} immutable ObjectRef fields must be provided together"
        )
    return _control_object_ref(parsed, prefix, media_type=media_type)


def _normalize_run_ids(
    values: Sequence[str],
    *,
    label: str,
) -> tuple[str, ...]:
    if not values:
        raise ValueError(f"{label} requires at least one run ID")
    if len(values) > MAX_EXPLICIT_RUN_IDS:
        raise ValueError(f"{label} supports at most {MAX_EXPLICIT_RUN_IDS} run IDs")
    normalized = tuple(require_sha256(value, label="run_id") for value in values)
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{label} contains duplicate run IDs")
    return tuple(sorted(normalized))


def _parse_source_watermarks(values: Sequence[str]) -> dict[str, str]:
    watermarks: dict[str, str] = {}
    for value in values:
        source, separator, watermark = value.partition("=")
        if not separator:
            raise ValueError("source watermark must use SOURCE_PRODUCT_ID=VALUE")
        source_product_id = require_slug(source, label="source_product_id")
        normalized = watermark.strip()
        if not normalized or len(normalized) > 1024:
            raise ValueError("source watermark must be non-empty and bounded")
        if source_product_id in watermarks:
            raise ValueError(f"duplicate source watermark: {source_product_id}")
        watermarks[source_product_id] = normalized
    return dict(sorted(watermarks.items()))


def _validate_source_watermark_changes(
    *,
    parent: CommunitySilverEpochManifest,
    source_watermarks: Mapping[str, str],
    delta_runs: Mapping[str, CommunityIngestRun],
) -> None:
    changed_watermarks = {
        source_product_id
        for source_product_id in (
            set(parent.source_watermarks) | set(source_watermarks)
        )
        if parent.source_watermarks.get(source_product_id)
        != source_watermarks.get(source_product_id)
    }
    delta_source_products = {
        run.source_product_id
        for run in delta_runs.values()
        if run.run_kind == IngestRunKind.SOURCE_ASSERTIONS
    }
    unexplained = sorted(changed_watermarks - delta_source_products)
    if unexplained:
        raise ValueError(
            "source watermarks changed without a source delta run: "
            + ", ".join(unexplained)
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
        s3_credentials_provider=parsed.s3_credentials_provider,
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
    spark = builder.getOrCreate()
    checkpoint_root = (
        f"{config.warehouse.rstrip('/')}/research/control/spark-checkpoints/"
        f"{spark.sparkContext.applicationId}"
    )
    spark.sparkContext.setCheckpointDir(checkpoint_root)
    return spark


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
    """Read one bounded immutable control object into a validated model."""

    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="research-silver-control-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "object.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        return model.model_validate_json(materialized.path.read_bytes())


def _read_silver_manifest(
    store: RuntimeObjectStore,
    reference: ObjectRef,
) -> CommunitySilverManifest:
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="research-silver-control-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "silver-manifest.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        manifest = parse_community_silver_manifest(materialized.path.read_bytes())
    expected_media_type = (
        SILVER_EPOCH_MEDIA_TYPE
        if isinstance(manifest, CommunitySilverEpochManifest)
        else SILVER_SNAPSHOT_MEDIA_TYPE
    )
    if reference.media_type != expected_media_type:
        raise ValueError("Silver manifest media type does not match schema version")
    return manifest


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


def _tables_for_silver_manifest(
    spark: Any,
    config: CatalogConfig,
    manifest: CommunitySilverManifest,
) -> CommunityCatalogTables:
    return CommunityCatalogTables(
        spark,
        config,
        identity_generation_id=manifest.identity_generation_id,
        table_mapping=community_silver_table_mapping(manifest),
    )


def _optional_identity_generation_id(value: str | None) -> str | None:
    if value is None:
        return None
    return require_identity_generation_id(value)


def _validate_run_generations(
    runs: Mapping[str, CommunityIngestRun],
    *,
    identity_generation_id: str | None,
    table_mapping: Mapping[str, str],
) -> None:
    """Reject Identity control rows from another physical generation."""

    for run_id, run in runs.items():
        if run.run_kind == IngestRunKind.SOURCE_ASSERTIONS:
            continue
        declared_generation = run.input_manifest.get("identityGenerationId")
        declared_mapping = run.input_manifest.get("tableMapping")
        expected_mapping = (
            None if identity_generation_id is None else dict(table_mapping)
        )
        if (
            declared_generation != identity_generation_id
            or declared_mapping != expected_mapping
        ):
            raise ValueError(f"run {run_id} belongs to another Identity generation")


def _identity_parent_silver_binding(
    run: CommunityIngestRun,
) -> tuple[ObjectRef, str] | None:
    """Return the immutable Silver input that an Identity state change extends."""

    binding: Any
    expected_id: Any
    if run.run_kind == IngestRunKind.IDENTITY_RESOLUTION:
        identity_mode = run.input_manifest.get("identityMode")
        if identity_mode == "full":
            return None
        if identity_mode != "incremental":
            raise ValueError(f"Identity run {run.run_id} has an invalid identityMode")
        pinned_inputs = run.input_manifest.get("pinnedInputs")
        if not isinstance(pinned_inputs, Mapping):
            raise ValueError(
                f"incremental Identity run {run.run_id} lacks pinnedInputs"
            )
        binding = pinned_inputs.get("silverSnapshot")
        if not isinstance(binding, Mapping):
            raise ValueError(
                f"incremental Identity run {run.run_id} lacks a pinned Silver input"
            )
        snapshot_id = binding.get("snapshotSetId")
        epoch_id = binding.get("epochId")
        if (snapshot_id is None) == (epoch_id is None):
            raise ValueError(
                f"incremental Identity run {run.run_id} has an invalid Silver binding"
            )
        expected_id = snapshot_id if snapshot_id is not None else epoch_id
    elif run.run_kind == IngestRunKind.IDENTITY_CURATION:
        binding = run.input_manifest.get("pinnedSilverSnapshot")
        if not isinstance(binding, Mapping):
            raise ValueError(
                f"Identity curation run {run.run_id} lacks a pinned Silver input"
            )
        expected_id = binding.get("snapshotSetId")
    else:
        return None

    raw_reference = binding.get("object")
    if not isinstance(raw_reference, Mapping):
        raise ValueError(f"Identity run {run.run_id} lacks a Silver ObjectRef")
    try:
        reference = ObjectRef.model_validate(raw_reference)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Identity run {run.run_id} has an invalid Silver ObjectRef"
        ) from exc
    if (
        reference.format != "OBJECT_FORMAT_JSON"
        or reference.media_type
        not in {SILVER_SNAPSHOT_MEDIA_TYPE, SILVER_EPOCH_MEDIA_TYPE}
        or not 0 < reference.size_bytes <= CONTROL_MAX_BYTES
    ):
        raise ValueError(f"Identity run {run.run_id} has an invalid Silver ObjectRef")
    _validate_object_uri(reference.uri, label="Identity parent Silver URI")
    if urlsplit(reference.uri).scheme == "s3" and (
        reference.object_version is None or reference.etag is None
    ):
        raise ValueError(f"Identity run {run.run_id} has a mutable Silver ObjectRef")
    return reference, require_sha256(
        str(expected_id or ""),
        label="Identity parent Silver manifest ID",
    )


def _explicit_silver_run_ids(
    manifest: CommunitySilverManifest,
    *,
    read_manifest: Callable[[ObjectRef], CommunitySilverManifest],
) -> tuple[str, ...]:
    """Enumerate the exact run history declared by a snapshot or epoch chain."""

    if isinstance(manifest, CommunitySilverSnapshotSet):
        return manifest.committed_run_ids

    expected_count = manifest.committed_run_count
    generation = manifest.identity_generation_id
    table_mapping = community_silver_table_mapping(manifest)
    run_ids: set[str] = set()
    seen_epochs: set[str] = set()
    current = manifest
    while True:
        if current.epoch_id in seen_epochs:
            raise ValueError("Silver epoch lineage contains a cycle")
        seen_epochs.add(current.epoch_id)
        overlap = run_ids.intersection(current.delta_run_ids)
        if overlap:
            raise ValueError("Silver epoch lineage repeats delta run IDs")
        run_ids.update(current.delta_run_ids)
        if current.parent_epoch is None:
            break
        parent_ref = current.parent_epoch
        parent = read_manifest(parent_ref.object_ref)
        if (
            not isinstance(parent, CommunitySilverEpochManifest)
            or parent.epoch_id != parent_ref.epoch_id
        ):
            raise ValueError("Silver epoch parent reference does not match its object")
        if (
            parent.identity_generation_id != generation
            or community_silver_table_mapping(parent) != table_mapping
        ):
            raise ValueError("Silver epoch lineage changes Identity generation")
        current = parent
    if len(run_ids) != expected_count:
        raise ValueError("Silver epoch lineage cannot enumerate its committed runs")
    return tuple(sorted(run_ids))


def _parentless_identity_lineage_run_ids(
    seed_runs: Mapping[str, CommunityIngestRun],
    *,
    identity_generation_id: str,
    table_mapping: Mapping[str, str],
    read_manifest: Callable[[ObjectRef], CommunitySilverManifest],
    load_runs: Callable[
        [CommunitySilverManifest, tuple[str, ...]],
        Mapping[str, CommunityIngestRun],
    ],
) -> tuple[str, ...]:
    """Resolve only the Identity ancestors pinned by parentless epoch runs."""

    seed_ids = set(seed_runs)
    lineage_ids: set[str] = set()
    visited: set[str] = set()
    pending = [
        run
        for run in seed_runs.values()
        if run.run_kind
        in {IngestRunKind.IDENTITY_RESOLUTION, IngestRunKind.IDENTITY_CURATION}
    ]
    while pending:
        run = pending.pop()
        if run.run_id in visited:
            continue
        visited.add(run.run_id)
        binding = _identity_parent_silver_binding(run)
        if binding is None:
            continue
        reference, expected_manifest_id = binding
        parent = read_manifest(reference)
        if community_silver_manifest_id(parent) != expected_manifest_id:
            raise ValueError(f"Identity run {run.run_id} pins another Silver manifest")
        if (
            parent.identity_generation_id != identity_generation_id
            or community_silver_table_mapping(parent) != dict(table_mapping)
        ):
            raise ValueError(
                f"Identity run {run.run_id} extends another Identity generation"
            )
        parent_run_ids = _explicit_silver_run_ids(
            parent,
            read_manifest=read_manifest,
        )
        parent_runs = dict(load_runs(parent, parent_run_ids))
        if set(parent_runs) != set(parent_run_ids):
            raise ValueError("Identity parent Silver manifest has missing run metadata")
        _validate_run_generations(
            parent_runs,
            identity_generation_id=identity_generation_id,
            table_mapping=table_mapping,
        )
        for parent_run in parent_runs.values():
            if parent_run.run_kind not in {
                IngestRunKind.IDENTITY_RESOLUTION,
                IngestRunKind.IDENTITY_CURATION,
            }:
                continue
            if parent_run.run_id == run.run_id:
                raise ValueError("Identity lineage contains a run cycle")
            if parent_run.run_id not in seed_ids:
                lineage_ids.add(parent_run.run_id)
            if parent_run.run_id not in visited:
                pending.append(parent_run)
    return tuple(sorted(lineage_ids))


def _publication_identity_lineage_run_ids(
    seed_runs: Mapping[str, CommunityIngestRun],
    *,
    parsed: argparse.Namespace,
    spark: Any,
    config: CatalogConfig,
    identity_generation_id: str | None,
    table_mapping: Mapping[str, str],
) -> tuple[str, ...]:
    """Close a publication's requested runs over pinned Identity ancestors."""

    if identity_generation_id is None or not seed_runs:
        return ()
    manifest_cache: dict[str, CommunitySilverManifest] = {}

    def read_lineage_manifest(reference: ObjectRef) -> CommunitySilverManifest:
        cache_key = canonical_json(
            reference.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            )
        )
        cached = manifest_cache.get(cache_key)
        if cached is not None:
            return cached
        lineage_store = _object_store(
            parsed,
            local_only=urlsplit(reference.uri).scheme == "file",
        )
        loaded = _read_silver_manifest(lineage_store, reference)
        manifest_cache[cache_key] = loaded
        return loaded

    def load_lineage_runs(
        manifest: CommunitySilverManifest,
        run_ids: tuple[str, ...],
    ) -> Mapping[str, CommunityIngestRun]:
        lineage_tables = _tables_for_silver_manifest(
            spark,
            config,
            manifest,
        )
        runs, _ = _load_run_state(
            spark,
            tables=lineage_tables,
            run_snapshot_id=manifest.run_snapshot_id,
            commit_snapshot_id=manifest.commit_snapshot_id,
            run_ids=run_ids,
        )
        return runs

    return _parentless_identity_lineage_run_ids(
        seed_runs,
        identity_generation_id=identity_generation_id,
        table_mapping=table_mapping,
        read_manifest=read_lineage_manifest,
        load_runs=load_lineage_runs,
    )


def _require_closed_identity_lineage(
    committed_run_ids: Sequence[str],
    identity_lineage_run_ids: Sequence[str],
) -> None:
    """Reject a Silver input that omits an Identity run's pinned ancestors."""

    missing = sorted(set(identity_lineage_run_ids) - set(committed_run_ids))
    if missing:
        raise ValueError(
            "Silver snapshot omits pinned Identity ancestors: " + ", ".join(missing)
        )


def _manifest_committed_runs(
    spark: Any,
    *,
    tables: CommunityCatalogTables,
    manifest: CommunitySilverManifest,
) -> Any:
    committed = (
        tables.generation_committed_runs_dataframe(
            run_snapshot_id=manifest.run_snapshot_id,
            commit_snapshot_id=manifest.commit_snapshot_id,
        )
        if manifest.identity_generation_id is not None
        else tables.committed_runs_dataframe(manifest.commit_snapshot_id)
    )
    if isinstance(manifest, CommunitySilverEpochManifest):
        if manifest.parent_epoch is None and manifest.delta_run_ids:
            committed = tables.select_committed_runs(
                committed,
                manifest.delta_run_ids,
                label="parentless Silver epoch",
            )
        tables.validate_epoch_committed_runs(manifest, committed)
        return committed
    if len(manifest.committed_run_ids) > MAX_EXPLICIT_RUN_IDS:
        raise ValueError("large Silver histories must use an epoch manifest")
    selected = spark.createDataFrame(
        [(run_id,) for run_id in manifest.committed_run_ids],
        "run_id STRING",
    )
    if selected.join(committed, "run_id", "left_anti").limit(1).count():
        raise ValueError("Silver snapshot set references an uncommitted run")
    return committed.join(selected, "run_id", "inner")


def _selected_silver_frames(
    *,
    tables: CommunityCatalogTables,
    snapshot: CommunitySilverManifest,
    committed_runs: Any,
    source_run_ids: tuple[str, ...],
    source_only: bool = False,
) -> dict[str, Any]:
    return tables.visible_dataframes(
        data_snapshot_ids=snapshot.data_snapshot_ids,
        commit_snapshot_id=snapshot.commit_snapshot_id,
        committed_runs=committed_runs,
        run_id_filters={table: source_run_ids for table in SOURCE_DATA_TABLES},
        selected_tables=tuple(SOURCE_DATA_TABLES) if source_only else None,
    )


def _incremental_unprocessed_source_run_ids(
    runs: Mapping[str, CommunityIngestRun],
) -> tuple[str, ...]:
    source_run_ids = {
        run_id
        for run_id, run in runs.items()
        if run.run_kind == IngestRunKind.SOURCE_ASSERTIONS
    }
    represented_source_run_ids: set[str] = set()
    identity_run_count = 0
    for run in runs.values():
        if run.run_kind != IngestRunKind.IDENTITY_RESOLUTION:
            continue
        identity_run_count += 1
        pinned_inputs = run.input_manifest.get("pinnedInputs")
        raw_source_run_ids = (
            pinned_inputs.get("sourceRunIds")
            if isinstance(pinned_inputs, Mapping)
            else None
        )
        if not isinstance(raw_source_run_ids, list):
            raise ValueError("baseline Identity run lacks pinned sourceRunIds")
        represented_source_run_ids.update(
            _normalize_run_ids(
                raw_source_run_ids,
                label="baseline Identity",
            )
        )
    if identity_run_count == 0:
        raise ValueError("incremental Identity requires a baseline Identity run")
    unknown_run_ids = represented_source_run_ids - source_run_ids
    if unknown_run_ids:
        raise ValueError(
            "baseline Identity references source runs absent from the pinned "
            "Silver snapshot: " + ", ".join(sorted(unknown_run_ids))
        )
    return tuple(sorted(source_run_ids - represented_source_run_ids))


def _incremental_unprocessed_source_runs_dataframe(runs: Any) -> Any:
    """Return an epoch's unprocessed Source runs without collecting its history."""

    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType

    required = {"run_id", "run_kind", "manifest_json"}
    if not required.issubset(getattr(runs, "columns", ())):
        raise ValueError("epoch run metadata is missing required columns")
    source_runs = runs.where(
        F.col("run_kind") == IngestRunKind.SOURCE_ASSERTIONS.value
    ).select("run_id")
    identity_runs = runs.where(
        F.col("run_kind") == IngestRunKind.IDENTITY_RESOLUTION.value
    ).withColumn(
        "_source_run_ids",
        F.from_json(
            F.get_json_object(
                "manifest_json",
                "$.inputManifest.pinnedInputs.sourceRunIds",
            ),
            ArrayType(StringType(), containsNull=False),
        ),
    )
    if not identity_runs.limit(1).count():
        raise ValueError("incremental Identity requires a baseline Identity run")
    if identity_runs.where(F.col("_source_run_ids").isNull()).limit(1).count():
        raise ValueError("baseline Identity run lacks pinned sourceRunIds")
    represented = identity_runs.select(
        F.explode("_source_run_ids").alias("run_id")
    ).dropDuplicates(["run_id"])
    if (
        represented.where(
            F.col("run_id").isNull() | ~F.col("run_id").rlike(r"^sha256:[0-9a-f]{64}$")
        )
        .limit(1)
        .count()
    ):
        raise ValueError("baseline Identity run has an invalid pinned source run ID")
    if represented.join(source_runs, "run_id", "left_anti").limit(1).count():
        raise ValueError(
            "baseline Identity references source runs absent from the pinned "
            "Silver epoch"
        )
    return source_runs.join(represented, "run_id", "left_anti")


def _run_identity(parsed: argparse.Namespace) -> dict[str, Any]:
    source_run_ids = _normalize_run_ids(
        parsed.source_run_ids,
        label="resolve-identity",
    )
    identity_generation_id = require_identity_generation_id(
        parsed.identity_generation_id
    )
    identity_mode = parsed.identity_mode
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
    resolution_config = IdentityResolutionConfig(
        max_exact_blocking_label_iterations=(parsed.identity_max_label_iterations),
        max_exact_blocking_component_size=(parsed.identity_max_component_size),
        max_exact_blocking_node_candidate_keys=(
            parsed.identity_max_node_candidate_keys
        ),
        max_exact_blocking_component_candidate_keys=(
            parsed.identity_max_component_candidate_keys
        ),
    )
    silver_ref = _control_object_ref(
        parsed,
        "silver_snapshot",
        media_type=parsed.silver_snapshot_media_type,
    )
    store = _object_store(
        parsed,
        local_only=urlsplit(silver_ref.uri).scheme == "file",
    )
    silver_snapshot = _read_silver_manifest(store, silver_ref)
    if (
        identity_mode == "incremental"
        and silver_snapshot.identity_generation_id != identity_generation_id
    ):
        raise ValueError("incremental Identity input belongs to another generation")
    if isinstance(silver_snapshot, CommunitySilverSnapshotSet):
        if len(silver_snapshot.committed_run_ids) > MAX_EXPLICIT_RUN_IDS:
            raise ValueError("large Silver histories must use an epoch manifest")
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
        input_tables = _tables_for_silver_manifest(
            spark,
            config,
            silver_snapshot,
        )
        output_tables = CommunityCatalogTables(
            spark,
            config,
            identity_generation_id=identity_generation_id,
        )
        if (
            identity_mode == "incremental"
            and input_tables.table_mapping != output_tables.table_mapping
        ):
            raise ValueError(
                "incremental Identity input table mapping differs from the "
                "active generation"
            )
        if identity_mode == "incremental":
            output_tables.assert_identity_snapshot_heads(
                silver_snapshot.data_snapshot_ids
            )
        committed_runs = _manifest_committed_runs(
            spark,
            tables=input_tables,
            manifest=silver_snapshot,
        )
        epoch_runs = (
            input_tables.visible_run_dataframe(
                run_snapshot_id=silver_snapshot.run_snapshot_id,
                committed_runs=committed_runs,
            )
            if isinstance(silver_snapshot, CommunitySilverEpochManifest)
            else None
        )
        source_runs_frame = spark.createDataFrame(
            [(run_id,) for run_id in source_run_ids],
            "run_id STRING",
        )
        if (
            source_runs_frame.join(
                committed_runs,
                "run_id",
                "left_anti",
            )
            .limit(1)
            .count()
        ):
            raise ValueError("source runs are absent from the pinned Silver manifest")
        state_run_ids = (
            source_run_ids
            if isinstance(silver_snapshot, CommunitySilverEpochManifest)
            else silver_snapshot.committed_run_ids
        )
        runs, _ = _load_run_state(
            spark,
            tables=input_tables,
            run_snapshot_id=silver_snapshot.run_snapshot_id,
            commit_snapshot_id=silver_snapshot.commit_snapshot_id,
            run_ids=state_run_ids,
        )
        if identity_mode == "incremental" and isinstance(
            silver_snapshot, CommunitySilverSnapshotSet
        ):
            identity_lineage_run_ids = _publication_identity_lineage_run_ids(
                runs,
                parsed=parsed,
                spark=spark,
                config=config,
                identity_generation_id=identity_generation_id,
                table_mapping=input_tables.table_mapping,
            )
            _require_closed_identity_lineage(
                silver_snapshot.committed_run_ids,
                identity_lineage_run_ids,
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
        if identity_mode == "incremental":
            if epoch_runs is None:
                delta_mismatch = (
                    source_run_ids != _incremental_unprocessed_source_run_ids(runs)
                )
            else:
                expected_delta = _incremental_unprocessed_source_runs_dataframe(
                    epoch_runs
                )
                delta_mismatch = bool(
                    expected_delta.join(
                        source_runs_frame,
                        "run_id",
                        "left_anti",
                    )
                    .limit(1)
                    .count()
                    or source_runs_frame.join(
                        expected_delta,
                        "run_id",
                        "left_anti",
                    )
                    .limit(1)
                    .count()
                )
            if delta_mismatch:
                raise ValueError(
                    "incremental Identity source runs differ from the unprocessed "
                    "Silver delta"
                )
        visible = _selected_silver_frames(
            tables=input_tables,
            snapshot=silver_snapshot,
            committed_runs=committed_runs,
            source_run_ids=source_run_ids,
            source_only=identity_mode == "full",
        )
        if identity_mode == "full":
            output_tables.create_tables()
            output_tables.assert_identity_tables_empty()
            for table in IDENTITY_TABLES:
                visible[table] = output_tables.empty_dataframe(table)
        silver_input = {
            "object": silver_ref.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
        }
        silver_input[
            (
                "epochId"
                if isinstance(silver_snapshot, CommunitySilverEpochManifest)
                else "snapshotSetId"
            )
        ] = community_silver_manifest_id(silver_snapshot)
        pinned_inputs = {
            "silverSnapshot": silver_input,
            "sourceRunIds": source_run_ids,
            "identityGenerationId": identity_generation_id,
            "identityMode": identity_mode,
            "tableMapping": output_tables.table_mapping,
        }
        registry = build_community_registry()
        source_policy = load_catalog_source_policy(registry=registry)
        if (
            parsed.source_policy_digest
            and parsed.source_policy_digest.lower() != source_policy.digest
        ):
            raise ValueError(
                "pinned source policy digest differs from the runtime contract"
            )
        pinned_inputs["sourcePolicyDigest"] = source_policy.digest
        input_id = deterministic_key(
            "community-identity-resolution-input-v2",
            pinned_inputs,
        )
        # Exercise the exact Iceberg snapshot/history metadata reads before
        # Identity resolution triggers checkpoints, shuffles, or counts.
        output_tables.preflight_snapshot_metadata(input_id)
        visible_runs = (
            epoch_runs
            if epoch_runs is not None
            else input_tables.visible_run_dataframe(
                run_snapshot_id=silver_snapshot.run_snapshot_id,
                committed_runs=committed_runs,
            )
        )
        ingest_runs = visible_runs.join(source_runs_frame, "run_id", "inner")
        run, frames = build_identity_resolution_dataframes(
            spark,
            visible_silver=visible,
            input_id=input_id,
            image_digest=image_digest,
            config_digest=config_digest,
            started_at=started_at,
            registry=registry,
            source_policy=source_policy,
            pinned_inputs=pinned_inputs,
            source_records=visible["community_source_record"],
            ingest_runs=ingest_runs,
            committed_source_run_ids=source_run_ids,
            resolution_config=resolution_config,
            identity_generation_id=identity_generation_id,
            identity_mode=identity_mode,
            table_mapping=output_tables.table_mapping,
        )
        commit = output_tables.stage_and_commit(
            run=run,
            dataframes=frames,
            committed_at=committed_at,
            identity_mode=identity_mode,
            expected_identity_snapshot_ids=(
                silver_snapshot.data_snapshot_ids
                if identity_mode == "incremental"
                else None
            ),
            pinned_identity_runs=(
                committed_runs if identity_mode == "incremental" else None
            ),
        )
        return {
            "context": "research",
            "stage": "identity-resolution",
            "runId": commit.run_id,
            "commitKey": commit.commit_key,
            "registryDigest": registry.digest,
            "sourcePolicyDigest": source_policy.digest,
            "configDigest": run.config_digest,
            "identityResolutionConfigDigest": resolution_config.digest,
            "identityGenerationId": identity_generation_id,
            "identityMode": identity_mode,
            "tableMapping": output_tables.table_mapping,
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


def _verify_epoch_data_counts(
    spark: Any,
    *,
    tables: CommunityCatalogTables,
    committed_runs: Any,
    commit_snapshot_id: int,
    data_snapshot_ids: dict[str, int | None],
) -> None:
    """Verify all historical run counts with distributed joins."""

    from pyspark.sql import functions as F

    commits = (
        spark.read.format("iceberg")
        .option("snapshot-id", str(commit_snapshot_id))
        .load(tables.table_name("community_ingest_commit"))
        .join(
            committed_runs.select("run_id").dropDuplicates(["run_id"]),
            "run_id",
            "inner",
        )
        .select("run_id", "table_counts_json")
    )
    for table in DATA_TABLE_COLUMNS:
        expected = commits.select(
            "run_id",
            F.get_json_object("table_counts_json", f"$.{table}")
            .cast("long")
            .alias("expected_count"),
        )
        if (
            expected.where(
                F.col("expected_count").isNull() | (F.col("expected_count") < 0)
            )
            .limit(1)
            .count()
        ):
            raise RuntimeError(f"{table} commit count is invalid")
        snapshot_id = data_snapshot_ids[table]
        if snapshot_id is None:
            if expected.where(F.col("expected_count") != 0).limit(1).count():
                raise RuntimeError(
                    f"{table} has committed rows but no containing snapshot"
                )
            continue
        actual = (
            spark.read.format("iceberg")
            .option("snapshot-id", str(snapshot_id))
            .load(tables.table_name(table))
            .join(
                committed_runs.select("run_id").dropDuplicates(["run_id"]),
                "run_id",
                "inner",
            )
            .groupBy("run_id")
            .count()
            .withColumnRenamed("count", "actual_count")
        )
        mismatch = (
            expected.join(actual, "run_id", "left")
            .fillna(0, subset=["actual_count"])
            .where(F.col("actual_count") != F.col("expected_count"))
        )
        if mismatch.limit(1).count():
            raise RuntimeError(f"{table} rows differ from committed epoch counts")


def _validate_epoch_delta(
    spark: Any,
    *,
    current_runs: Any,
    parent_runs: Any | None,
    delta_run_ids: tuple[str, ...],
) -> None:
    expected_delta = spark.createDataFrame(
        [(run_id,) for run_id in delta_run_ids],
        "run_id STRING",
    )
    if parent_runs is None:
        if not delta_run_ids:
            return
        actual_delta = current_runs.select("run_id").dropDuplicates(["run_id"])
    else:
        parent = parent_runs.select("run_id").dropDuplicates(["run_id"])
        current = current_runs.select("run_id").dropDuplicates(["run_id"])
        if parent.join(current, "run_id", "left_anti").limit(1).count():
            raise ValueError("parent epoch contains runs absent from the new epoch")
        actual_delta = current.join(parent, "run_id", "left_anti")
    if (
        actual_delta.join(expected_delta, "run_id", "left_anti").limit(1).count()
        or expected_delta.join(actual_delta, "run_id", "left_anti").limit(1).count()
    ):
        raise ValueError(
            "declared delta run IDs do not match epoch snapshot difference"
        )


def _run_epoch_publication(
    parsed: argparse.Namespace,
) -> dict[str, Any]:
    identity_generation_id = _optional_identity_generation_id(
        parsed.identity_generation_id
    )
    requested_delta_run_ids = (
        ()
        if not parsed.delta_run_ids
        else _normalize_run_ids(
            parsed.delta_run_ids,
            label="publish-epoch delta",
        )
    )
    if len(requested_delta_run_ids) > MAX_EPOCH_DELTA_RUNS:
        raise ValueError(
            f"publish-epoch delta supports at most {MAX_EPOCH_DELTA_RUNS} run IDs"
        )
    source_watermarks = _parse_source_watermarks(parsed.source_watermarks)
    created_at = require_rfc3339(parsed.created_at, label="created-at")
    destination_uri = _validate_object_uri(
        parsed.epoch_uri,
        label="epoch URI",
    )
    parent_ref = _optional_control_object_ref(
        parsed,
        "parent_epoch",
        media_type=SILVER_EPOCH_MEDIA_TYPE,
    )
    local_inputs = [destination_uri]
    if parent_ref is not None:
        local_inputs.append(parent_ref.uri)
    store = _object_store(
        parsed,
        local_only=all(urlsplit(uri).scheme == "file" for uri in local_inputs),
    )
    parent: CommunitySilverEpochManifest | None = None
    if parent_ref is not None:
        parent_manifest = _read_silver_manifest(store, parent_ref)
        if not isinstance(parent_manifest, CommunitySilverEpochManifest):
            raise ValueError(
                "parent epoch ObjectRef does not contain an epoch manifest"
            )
        parent = parent_manifest
        if parent.identity_generation_id != identity_generation_id:
            raise ValueError("parent epoch belongs to another Identity generation")
        if parse_rfc3339(created_at) < parse_rfc3339(parent.created_at):
            raise ValueError("epoch created-at must not precede its parent")
        missing_watermarks = sorted(
            set(parent.source_watermarks) - set(source_watermarks)
        )
        if missing_watermarks:
            raise ValueError(
                "epoch cannot remove parent source watermarks: "
                + ", ".join(missing_watermarks)
            )

    config = _catalog_config(parsed)
    spark = _spark_session(parsed, config)
    try:
        tables = CommunityCatalogTables(
            spark,
            config,
            identity_generation_id=identity_generation_id,
        )
        if (
            parent is not None
            and community_silver_table_mapping(parent) != tables.table_mapping
        ):
            raise ValueError("parent epoch table mapping differs from this publication")
        commit_snapshot_id = tables.latest_snapshot_id("community_ingest_commit")
        if commit_snapshot_id is None:
            raise ValueError("community_ingest_commit has no committed snapshot")
        run_snapshot_id = tables.latest_snapshot_id("community_ingest_run")
        if run_snapshot_id is None:
            raise ValueError("community_ingest_run has no committed snapshot")
        data_snapshot_ids = {
            table: tables.latest_snapshot_id(table) for table in DATA_TABLE_COLUMNS
        }
        available_committed_runs = (
            tables.generation_committed_runs_dataframe(
                run_snapshot_id=run_snapshot_id,
                commit_snapshot_id=commit_snapshot_id,
            )
            if identity_generation_id is not None
            else tables.committed_runs_dataframe(commit_snapshot_id)
        )

        requested_delta_runs: dict[str, CommunityIngestRun] = {}
        if requested_delta_run_ids:
            requested_delta_runs, _ = _load_run_state(
                spark,
                tables=tables,
                run_snapshot_id=run_snapshot_id,
                commit_snapshot_id=commit_snapshot_id,
                run_ids=requested_delta_run_ids,
            )
            _validate_run_generations(
                requested_delta_runs,
                identity_generation_id=identity_generation_id,
                table_mapping=tables.table_mapping,
            )

        parent_runs = None
        parent_epoch_ref = None
        baseline_epoch_ref = None
        if parent is not None:
            assert parent_ref is not None
            parent_runs = (
                tables.generation_committed_runs_dataframe(
                    run_snapshot_id=parent.run_snapshot_id,
                    commit_snapshot_id=parent.commit_snapshot_id,
                )
                if identity_generation_id is not None
                else tables.committed_runs_dataframe(parent.commit_snapshot_id)
            )
            tables.validate_epoch_committed_runs(parent, parent_runs)
            parent_epoch_ref = CommunitySilverEpochReference(
                epoch_id=parent.epoch_id,
                object_ref=parent_ref,
            )
            baseline_epoch_ref = parent.baseline_epoch or parent_epoch_ref
        identity_lineage_run_ids: tuple[str, ...] = ()
        if parent is None:
            identity_lineage_run_ids = _publication_identity_lineage_run_ids(
                requested_delta_runs,
                parsed=parsed,
                spark=spark,
                config=config,
                identity_generation_id=identity_generation_id,
                table_mapping=tables.table_mapping,
            )
        delta_run_ids = tuple(
            sorted({*requested_delta_run_ids, *identity_lineage_run_ids})
        )
        if len(delta_run_ids) > MAX_EPOCH_DELTA_RUNS:
            raise ValueError(
                "parentless Silver epoch Identity lineage exceeds "
                f"{MAX_EPOCH_DELTA_RUNS} run IDs"
            )
        committed_runs = available_committed_runs
        if parent is None and delta_run_ids:
            committed_runs = tables.select_committed_runs(
                available_committed_runs,
                delta_run_ids,
                label="parentless Silver epoch",
            )
        _validate_epoch_delta(
            spark,
            current_runs=committed_runs,
            parent_runs=parent_runs,
            delta_run_ids=delta_run_ids,
        )
        committed_run_count, committed_run_digest = tables.committed_run_summary(
            committed_runs
        )
        if committed_run_count == 0:
            raise ValueError("Silver epoch requires at least one committed run")
        tables.visible_run_dataframe(
            run_snapshot_id=run_snapshot_id,
            committed_runs=committed_runs,
        )

        delta_runs: dict[str, CommunityIngestRun] = {}
        if delta_run_ids:
            delta_runs, delta_commits = _load_run_state(
                spark,
                tables=tables,
                run_snapshot_id=run_snapshot_id,
                commit_snapshot_id=commit_snapshot_id,
                run_ids=delta_run_ids,
            )
            _validate_run_generations(
                delta_runs,
                identity_generation_id=identity_generation_id,
                table_mapping=tables.table_mapping,
            )
            _verify_data_counts(
                spark,
                tables=tables,
                run_ids=delta_run_ids,
                commits=delta_commits,
                data_snapshot_ids=data_snapshot_ids,
            )
        if parent is not None:
            _validate_source_watermark_changes(
                parent=parent,
                source_watermarks=source_watermarks,
                delta_runs=delta_runs,
            )
        _verify_epoch_data_counts(
            spark,
            tables=tables,
            committed_runs=committed_runs,
            commit_snapshot_id=commit_snapshot_id,
            data_snapshot_ids=data_snapshot_ids,
        )
        epoch = build_community_silver_epoch_manifest(
            parent_epoch=parent_epoch_ref,
            baseline_epoch=baseline_epoch_ref,
            delta_run_ids=delta_run_ids,
            run_snapshot_id=run_snapshot_id,
            commit_snapshot_id=commit_snapshot_id,
            data_snapshot_ids=data_snapshot_ids,
            identity_generation_id=identity_generation_id,
            table_mapping=(
                tables.table_mapping if identity_generation_id is not None else None
            ),
            source_watermarks=source_watermarks,
            committed_run_count=committed_run_count,
            committed_run_digest=committed_run_digest,
            created_at=created_at,
        )
        result = store.upload_bytes(
            epoch.json_bytes(),
            destination_uri,
            media_type=SILVER_EPOCH_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=CONTROL_MAX_BYTES,
        )
        reference = result.object_ref
        _require_immutable_snapshot_output(reference)
        store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
        return {
            "context": "research",
            "stage": "silver-epoch-publication",
            "epochId": epoch.epoch_id,
            "parentEpochId": (
                None if epoch.parent_epoch is None else epoch.parent_epoch.epoch_id
            ),
            "baselineEpochId": (
                None if epoch.baseline_epoch is None else epoch.baseline_epoch.epoch_id
            ),
            "requestedDeltaRunIds": requested_delta_run_ids,
            "identityLineageRunIds": identity_lineage_run_ids,
            "deltaRunIds": epoch.delta_run_ids,
            "sourceWatermarks": epoch.source_watermarks,
            "committedRunCount": epoch.committed_run_count,
            "committedRunDigest": epoch.committed_run_digest,
            "runSnapshotId": epoch.run_snapshot_id,
            "commitSnapshotId": epoch.commit_snapshot_id,
            "dataSnapshotIds": epoch.data_snapshot_ids,
            **(
                {
                    "identityGenerationId": epoch.identity_generation_id,
                    "tableMapping": epoch.table_mapping,
                }
                if epoch.identity_generation_id is not None
                else {}
            ),
            "silverEpoch": reference.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            "reused": result.reused,
        }
    finally:
        spark.stop()


def _run_snapshot_publication(
    parsed: argparse.Namespace,
) -> dict[str, Any]:
    identity_generation_id = _optional_identity_generation_id(
        parsed.identity_generation_id
    )
    requested_run_ids = _normalize_run_ids(
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
        tables = CommunityCatalogTables(
            spark,
            config,
            identity_generation_id=identity_generation_id,
        )
        if parsed.initialize_identity_generation:
            if identity_generation_id is None:
                raise ValueError(
                    "Identity generation initialization requires a generation ID"
                )
            tables.create_tables()
            tables.assert_identity_tables_empty()
        commit_snapshot_id = tables.latest_snapshot_id("community_ingest_commit")
        if commit_snapshot_id is None:
            raise ValueError("community_ingest_commit has no committed snapshot")
        run_snapshot_id = tables.latest_snapshot_id("community_ingest_run")
        if run_snapshot_id is None:
            raise ValueError("community_ingest_run has no committed snapshot")
        data_snapshot_ids = {
            table: tables.latest_snapshot_id(table) for table in DATA_TABLE_COLUMNS
        }
        requested_runs, commits = _load_run_state(
            spark,
            tables=tables,
            run_snapshot_id=run_snapshot_id,
            commit_snapshot_id=commit_snapshot_id,
            run_ids=requested_run_ids,
        )
        _validate_run_generations(
            requested_runs,
            identity_generation_id=identity_generation_id,
            table_mapping=tables.table_mapping,
        )
        identity_lineage_run_ids = _publication_identity_lineage_run_ids(
            requested_runs,
            parsed=parsed,
            spark=spark,
            config=config,
            identity_generation_id=identity_generation_id,
            table_mapping=tables.table_mapping,
        )
        run_ids = tuple(sorted({*requested_run_ids, *identity_lineage_run_ids}))
        if len(run_ids) > MAX_EXPLICIT_RUN_IDS:
            raise ValueError(
                "publish-snapshot Identity lineage exceeds "
                f"{MAX_EXPLICIT_RUN_IDS} run IDs"
            )
        runs = requested_runs
        if identity_lineage_run_ids:
            runs, commits = _load_run_state(
                spark,
                tables=tables,
                run_snapshot_id=run_snapshot_id,
                commit_snapshot_id=commit_snapshot_id,
                run_ids=run_ids,
            )
            _validate_run_generations(
                runs,
                identity_generation_id=identity_generation_id,
                table_mapping=tables.table_mapping,
            )
        unprocessed_identity_source_run_ids = (
            _incremental_unprocessed_source_run_ids(runs)
            if any(
                run.run_kind == IngestRunKind.IDENTITY_RESOLUTION
                for run in runs.values()
            )
            else ()
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
            identity_generation_id=identity_generation_id,
            table_mapping=(
                tables.table_mapping if identity_generation_id is not None else None
            ),
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
            "requestedRunIds": requested_run_ids,
            "identityLineageRunIds": identity_lineage_run_ids,
            "unprocessedIdentitySourceRunIds": (unprocessed_identity_source_run_ids),
            "identityGenerationInitialized": bool(
                parsed.initialize_identity_generation
            ),
            "committedRunIds": snapshot.committed_run_ids,
            "runSnapshotId": snapshot.run_snapshot_id,
            "commitSnapshotId": snapshot.commit_snapshot_id,
            "dataSnapshotIds": snapshot.data_snapshot_ids,
            **(
                {
                    "identityGenerationId": snapshot.identity_generation_id,
                    "tableMapping": snapshot.table_mapping,
                }
                if snapshot.identity_generation_id is not None
                else {}
            ),
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
