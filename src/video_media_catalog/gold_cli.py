"""Snapshot-pinned distributed Silver-to-Gold Spark worker."""

from __future__ import annotations

import argparse
import re
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, sha256_digest
from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_release import ReleasePolicyContext
from video_media_catalog.community_snapshot import CommunitySilverSnapshotSet
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.gold import community_display_policy
from video_media_catalog.gold_iceberg import CommunityGoldTables
from video_media_catalog.gold_ingest import (
    ATTRIBUTION_MEDIA_TYPE,
    GOLD_QUALITY_MEDIA_TYPE,
)
from video_media_catalog.gold_quality import GoldQualityStatus
from video_media_catalog.gold_spark_transform import build_distributed_gold
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.rights import PolicyZone
from video_media_catalog.runtime_args import join_uri

CONTROL_MAX_BYTES = 16 * 1024 * 1024
SILVER_SNAPSHOT_MEDIA_TYPE = (
    "application/vnd.video-media-catalog.silver-snapshot-set.v2+json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-gold-spark",
        description=(
            "Build one policy-specific Gold release from exact Silver snapshots."
        ),
    )
    parser.add_argument("--silver-snapshot-uri", required=True)
    parser.add_argument("--silver-snapshot-hash", required=True)
    parser.add_argument("--silver-snapshot-size", type=int, required=True)
    parser.add_argument("--silver-snapshot-version", default="")
    parser.add_argument("--silver-snapshot-etag", default="")
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--planned-at", required=True)
    parser.add_argument("--committed-at", required=True)
    parser.add_argument("--catalog-name", default="media")
    parser.add_argument("--silver-namespace", default="community_catalog_v2")
    parser.add_argument("--gold-namespace", default="community_gold_v2")
    parser.add_argument(
        "--catalog-type",
        choices=("hadoop", "glue"),
        default="glue",
    )
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--aws-region")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-path-style-access", action="store_true")
    parser.add_argument("--master")
    parser.add_argument("--app-name", default="community-catalog-gold-v2")
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--spark-packages")
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--context-id", default="public-sharealike")
    parser.add_argument("--audience", default="public")
    parser.add_argument("--purpose", default="catalog")
    parser.add_argument("--territories", default="*")
    parser.add_argument(
        "--allowed-zones",
        default="open_cc0,open_attributed,open_sharealike,public_registry",
    )
    parser.add_argument("--max-conflict-ratio", type=float, default=0.05)
    parser.add_argument(
        "--max-unresolved-identity-ratio",
        type=float,
        default=0.05,
    )
    parser.add_argument("--max-redirect-hops", type=int, default=16)
    return parser


def _snapshot_ref(parsed: argparse.Namespace) -> ObjectRef:
    match = re.fullmatch(
        r"(?:sha256:hex:|sha256:)?([0-9a-fA-F]{64})",
        parsed.silver_snapshot_hash,
    )
    if match is None:
        raise ValueError("silver snapshot hash must contain 64 hex digits")
    if not 0 < parsed.silver_snapshot_size <= CONTROL_MAX_BYTES:
        raise ValueError("silver snapshot size must be between 1 byte and 16 MiB")
    scheme = urlsplit(parsed.silver_snapshot_uri).scheme
    if scheme not in {"file", "s3"}:
        raise ValueError("silver snapshot URI must use file:// or s3://")
    version = parsed.silver_snapshot_version.strip() or None
    etag = parsed.silver_snapshot_etag.strip().strip('"') or None
    if scheme == "s3" and (version is None or etag is None):
        raise ValueError("S3 silver snapshot requires version and ETag")
    return ObjectRef(
        uri=parsed.silver_snapshot_uri,
        format="OBJECT_FORMAT_JSON",
        media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
        checksum=Checksum(value=match.group(1).lower()),
        size_bytes=parsed.silver_snapshot_size,
        etag=etag,
        object_version=version,
    )


def _read_snapshot(store, reference: ObjectRef) -> CommunitySilverSnapshotSet:
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="community-silver-snapshot-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "snapshot.json",
            max_bytes=CONTROL_MAX_BYTES,
        )
        return CommunitySilverSnapshotSet.model_validate_json(
            materialized.path.read_bytes()
        )


def _parse_values(value: str) -> tuple[str, ...]:
    result = tuple(sorted({item.strip() for item in value.split(",") if item.strip()}))
    if not result:
        raise ValueError("comma-separated configuration must not be empty")
    return result


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    if not 0 <= parsed.max_conflict_ratio <= 1:
        raise ValueError("max-conflict-ratio must be between 0 and 1")
    if not 0 <= parsed.max_unresolved_identity_ratio <= 1:
        raise ValueError("max-unresolved-identity-ratio must be between 0 and 1")
    if parsed.max_redirect_hops < 1:
        raise ValueError("max-redirect-hops must be positive")
    if urlsplit(parsed.output_prefix).scheme not in {"file", "s3"}:
        raise ValueError("output-prefix must use file:// or s3://")
    reference = _snapshot_ref(parsed)
    local = (
        urlsplit(reference.uri).scheme == "file"
        and urlsplit(parsed.output_prefix).scheme == "file"
    )
    store = BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=object() if local else None,
    )
    snapshot = _read_snapshot(store, reference)
    silver_config = CatalogConfig(
        catalog_name=parsed.catalog_name,
        namespace=parsed.silver_namespace,
        warehouse=parsed.warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
    )
    gold_config = CatalogConfig(
        catalog_name=parsed.catalog_name,
        namespace=parsed.gold_namespace,
        warehouse=parsed.warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
    )
    zones = tuple(PolicyZone(item) for item in _parse_values(parsed.allowed_zones))
    context = ReleasePolicyContext(
        context_id=parsed.context_id,
        audience=parsed.audience,
        purpose=parsed.purpose,
        territories=_parse_values(parsed.territories),
        as_of=parsed.planned_at,
        allowed_zones=zones,
    )
    policy = community_display_policy().model_copy(
        update={
            "max_conflict_ratio": parsed.max_conflict_ratio,
            "max_unresolved_identity_ratio": (parsed.max_unresolved_identity_ratio),
        }
    )
    nonsecret_config = {
        "silverNamespace": parsed.silver_namespace,
        "goldNamespace": parsed.gold_namespace,
        "catalogName": parsed.catalog_name,
        "catalogType": parsed.catalog_type,
        "warehouse": parsed.warehouse,
        "context": context.model_dump(mode="json", by_alias=True),
        "fieldPolicyDigest": policy.digest,
        "maxRedirectHops": parsed.max_redirect_hops,
    }
    config_digest = sha256_digest(canonical_json(nonsecret_config))
    resolver_digest = sha256_digest("community-gold-spark-v1")

    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(parsed.app_name)
    if parsed.master:
        builder = builder.master(parsed.master)
    builder = silver_config.configure_builder(builder)
    if parsed.shuffle_partitions is not None:
        builder = builder.config(
            "spark.sql.shuffle.partitions",
            str(parsed.shuffle_partitions),
        )
    if parsed.spark_packages:
        builder = builder.config("spark.jars.packages", parsed.spark_packages)
    spark = builder.getOrCreate()
    build = None
    try:
        silver_tables = CommunityCatalogTables(spark, silver_config)
        visible = silver_tables.visible_dataframes(
            data_snapshot_ids=snapshot.data_snapshot_ids,
            commit_snapshot_id=snapshot.commit_snapshot_id,
        )
        selected_runs = spark.createDataFrame(
            [(run_id,) for run_id in snapshot.committed_run_ids],
            "run_id STRING",
        )
        committed_runs = (
            spark.read.format("iceberg")
            .option("snapshot-id", str(snapshot.commit_snapshot_id))
            .load(silver_tables.table_name("community_ingest_commit"))
            .select("run_id")
            .dropDuplicates(["run_id"])
        )
        if (
            selected_runs.join(
                committed_runs,
                "run_id",
                "left_anti",
            )
            .limit(1)
            .count()
        ):
            raise ValueError("Silver snapshot set references an uncommitted run")
        visible = {
            table: frame.join(selected_runs, "run_id", "inner")
            for table, frame in visible.items()
        }
        build = build_distributed_gold(
            spark,
            visible_silver=visible,
            registry=build_community_registry(),
            policy_context=context,
            field_policy=policy,
            committed_run_ids=snapshot.committed_run_ids,
            silver_snapshot_ids=snapshot.data_snapshot_ids,
            identity_snapshot_ids={
                table: snapshot.data_snapshot_ids[table]
                for table in (
                    "community_entity_ledger",
                    "community_entity_membership",
                    "community_entity_redirect",
                )
            },
            resolver_digest=resolver_digest,
            image_digest=parsed.image_digest,
            config_digest=config_digest,
            planned_at=parsed.planned_at,
            max_redirect_hops=parsed.max_redirect_hops,
        )
        plan_prefix = join_uri(
            parsed.output_prefix,
            "gold-plans",
            build.plan.release_plan_id.removeprefix("sha256:"),
        )
        quality_ref = store.upload_bytes(
            build.quality_report.json_bytes(),
            join_uri(plan_prefix, "quality-report.json"),
            media_type=GOLD_QUALITY_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=CONTROL_MAX_BYTES,
        ).object_ref
        if build.quality_report.status != GoldQualityStatus.PASS:
            raise ValueError("Gold quality gate failed")
        attribution_ref = store.upload_bytes(
            build.attribution_manifest.json_bytes(),
            join_uri(plan_prefix, "attribution-manifest.json"),
            media_type=ATTRIBUTION_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=CONTROL_MAX_BYTES,
        ).object_ref
        commit = CommunityGoldTables(spark, gold_config).stage_and_commit(
            plan=build.plan,
            dataframes=build.dataframes,
            quality_report=build.quality_report,
            quality_report_ref=quality_ref,
            attribution_manifest=build.attribution_manifest,
            attribution_manifest_ref=attribution_ref,
            committed_at=parsed.committed_at,
        )
        return {
            "releasePlanId": build.plan.release_plan_id,
            "commitKey": commit.commit_key,
            "qualityReport": quality_ref.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
            "attributionManifest": attribution_ref.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
            "tableCounts": commit.table_counts,
            "tableSnapshotIds": commit.table_snapshot_ids,
        }
    finally:
        if build is not None:
            build.unpersist()
        spark.stop()


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
