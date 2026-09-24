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
from video_media_catalog.community_snapshot import (
    CONTROL_MAX_BYTES,
    MAX_EPOCH_DELTA_RUNS,
    SILVER_EPOCH_MEDIA_TYPE,
    SILVER_SNAPSHOT_MEDIA_TYPE,
    CommunitySilverEpochManifest,
    CommunitySilverManifest,
    CommunitySilverSnapshotSet,
    parse_community_silver_manifest,
)
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.gold import (
    research_context,
    research_policy,
)
from video_media_catalog.gold_freshness import research_release_freshness_policy
from video_media_catalog.gold_iceberg import CommunityGoldTables
from video_media_catalog.gold_ingest import (
    ATTRIBUTION_MEDIA_TYPE,
    GOLD_QUALITY_MEDIA_TYPE,
    GOLD_RELEASE_COMMIT_MEDIA_TYPE,
)
from video_media_catalog.gold_quality import (
    GoldBuildMode,
    GoldQualityStatus,
)
from video_media_catalog.gold_spark_transform import build_distributed_gold
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.rights import RightsTerminationFence
from video_media_catalog.runtime_args import join_uri


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-gold-spark",
        description="Build the shared authenticated research Gold release.",
    )
    parser.add_argument("--silver-snapshot-uri", required=True)
    parser.add_argument("--silver-snapshot-hash", required=True)
    parser.add_argument("--silver-snapshot-size", type=int, required=True)
    parser.add_argument("--silver-snapshot-version", default="")
    parser.add_argument("--silver-snapshot-etag", default="")
    parser.add_argument(
        "--silver-snapshot-media-type",
        choices=(SILVER_SNAPSHOT_MEDIA_TYPE, SILVER_EPOCH_MEDIA_TYPE),
        default=SILVER_SNAPSHOT_MEDIA_TYPE,
    )
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--planned-at", required=True)
    parser.add_argument("--committed-at", required=True)
    parser.add_argument("--catalog-name", default="media")
    parser.add_argument("--silver-namespace", default="video_media_catalog")
    parser.add_argument("--gold-namespace", default="video_media_catalog")
    parser.add_argument(
        "--catalog-type",
        choices=("hadoop", "glue"),
        default="glue",
    )
    parser.add_argument("--warehouse", required=True)
    parser.add_argument("--aws-region")
    parser.add_argument("--s3-endpoint")
    parser.add_argument("--s3-path-style-access", action="store_true")
    parser.add_argument(
        "--s3-credentials-provider",
        choices=("web-identity", "default"),
        default="web-identity",
    )
    parser.add_argument("--master")
    parser.add_argument("--app-name", default="media-catalog-research-gold")
    parser.add_argument("--shuffle-partitions", type=int)
    parser.add_argument("--spark-packages")
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--territories", default="*")
    parser.add_argument("--max-conflict-ratio", type=float, default=0.05)
    parser.add_argument(
        "--max-unresolved-identity-ratio",
        type=float,
        default=0.05,
    )
    parser.add_argument("--max-redirect-hops", type=int, default=16)
    parser.add_argument(
        "--build-mode",
        choices=("release", "candidate-backfill"),
        default="release",
    )
    parser.add_argument("--tmdb-freshness-slo-hours", type=int, default=36)
    parser.add_argument("--tvmaze-freshness-slo-hours", type=int, default=36)
    parser.add_argument("--imdb-freshness-slo-hours", type=int, default=10 * 24)
    parser.add_argument("--wikidata-freshness-slo-hours", type=int, default=45 * 24)
    parser.add_argument(
        "--termination-fence-json",
        type=Path,
        action="append",
        default=[],
    )
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
        media_type=parsed.silver_snapshot_media_type,
        checksum=Checksum(value=match.group(1).lower()),
        size_bytes=parsed.silver_snapshot_size,
        etag=etag,
        object_version=version,
    )


def _read_snapshot(store, reference: ObjectRef) -> CommunitySilverManifest:
    store.verify(reference, max_bytes=CONTROL_MAX_BYTES)
    with tempfile.TemporaryDirectory(prefix="community-silver-snapshot-") as directory:
        materialized = store.download(
            reference,
            Path(directory) / "snapshot.json",
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


def _parse_values(value: str) -> tuple[str, ...]:
    result = tuple(sorted({item.strip() for item in value.split(",") if item.strip()}))
    if not result:
        raise ValueError("comma-separated configuration must not be empty")
    return result


def _read_termination_fence(path: Path) -> RightsTerminationFence:
    payload = path.read_bytes()
    if not 0 < len(payload) <= CONTROL_MAX_BYTES:
        raise ValueError("termination fence must be between 1 byte and 16 MiB")
    return RightsTerminationFence.model_validate_json(payload)


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
    if (
        isinstance(snapshot, CommunitySilverSnapshotSet)
        and len(snapshot.committed_run_ids) > MAX_EPOCH_DELTA_RUNS
    ):
        raise ValueError("large Silver histories must use an epoch manifest")
    registry = build_community_registry()
    termination_fences = tuple(
        _read_termination_fence(path) for path in parsed.termination_fence_json
    )
    if len({fence.fence_id for fence in termination_fences}) != len(termination_fences):
        raise ValueError("termination fence list contains duplicates")
    products = {
        product.source_product_id: product for product in registry.source_products
    }
    profiles = {profile.policy_id: profile for profile in registry.rights_profiles}
    for fence in termination_fences:
        product = products.get(fence.source_product_id)
        profile = profiles.get(fence.policy_id)
        if (
            product is None
            or profile is None
            or product.policy_id != fence.policy_id
            or profile.digest != fence.policy_digest
        ):
            raise ValueError("termination fence is not bound to the source registry")
    silver_config = CatalogConfig(
        catalog_name=parsed.catalog_name,
        namespace=parsed.silver_namespace,
        warehouse=parsed.warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
        s3_credentials_provider=parsed.s3_credentials_provider,
    )
    gold_config = CatalogConfig(
        catalog_name=parsed.catalog_name,
        namespace=parsed.gold_namespace,
        warehouse=parsed.warehouse,
        catalog_type=parsed.catalog_type,
        aws_region=parsed.aws_region,
        s3_endpoint=parsed.s3_endpoint,
        s3_path_style_access=parsed.s3_path_style_access,
        s3_credentials_provider=parsed.s3_credentials_provider,
    )
    context = research_context(
        as_of=parsed.planned_at,
        territories=_parse_values(parsed.territories),
    )
    policy = research_policy().model_copy(
        update={
            "max_conflict_ratio": parsed.max_conflict_ratio,
            "max_unresolved_identity_ratio": (parsed.max_unresolved_identity_ratio),
        }
    )
    freshness_policy = research_release_freshness_policy(
        tmdb_slo_hours=parsed.tmdb_freshness_slo_hours,
        tvmaze_slo_hours=parsed.tvmaze_freshness_slo_hours,
        imdb_slo_hours=parsed.imdb_freshness_slo_hours,
        wikidata_slo_hours=parsed.wikidata_freshness_slo_hours,
    )
    build_mode = (
        GoldBuildMode.CANDIDATE_BACKFILL
        if parsed.build_mode == "candidate-backfill"
        else GoldBuildMode.RELEASE
    )
    nonsecret_config = {
        "silverNamespace": parsed.silver_namespace,
        "goldNamespace": parsed.gold_namespace,
        "catalogName": parsed.catalog_name,
        "catalogType": parsed.catalog_type,
        "warehouse": parsed.warehouse,
        "context": context.model_dump(mode="json", by_alias=True),
        "fieldPolicyDigest": policy.digest,
        "releaseFreshnessPolicy": freshness_policy.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "buildMode": build_mode.value,
        "terminationFences": [
            fence.model_dump(mode="json", by_alias=True, exclude_none=True)
            for fence in sorted(termination_fences, key=lambda item: item.fence_id)
        ],
        "maxRedirectHops": parsed.max_redirect_hops,
    }
    config_digest = sha256_digest(canonical_json(nonsecret_config))
    resolver_digest = sha256_digest("community-gold-spark-v3")

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

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
        all_committed_runs = silver_tables.committed_runs_dataframe(
            snapshot.commit_snapshot_id
        )
        epoch_input = isinstance(snapshot, CommunitySilverEpochManifest)
        if epoch_input:
            silver_tables.validate_epoch_committed_runs(
                snapshot,
                all_committed_runs,
            )
            committed_runs = all_committed_runs
            committed_run_ids: tuple[str, ...] = ()
        else:
            assert isinstance(snapshot, CommunitySilverSnapshotSet)
            selected_runs = spark.createDataFrame(
                [(run_id,) for run_id in snapshot.committed_run_ids],
                "run_id STRING",
            )
            if (
                selected_runs.join(
                    all_committed_runs,
                    "run_id",
                    "left_anti",
                )
                .limit(1)
                .count()
            ):
                raise ValueError("Silver snapshot set references an uncommitted run")
            committed_runs = all_committed_runs.join(
                F.broadcast(selected_runs),
                "run_id",
                "left_semi",
            )
            committed_run_ids = snapshot.committed_run_ids
        visible = silver_tables.visible_dataframes(
            data_snapshot_ids=snapshot.data_snapshot_ids,
            commit_snapshot_id=snapshot.commit_snapshot_id,
            committed_runs=committed_runs,
            run_id_filters=(
                None
                if epoch_input
                else {
                    table: committed_run_ids
                    for table in DATA_TABLE_COLUMNS
                }
            ),
        )
        visible["community_ingest_run"] = silver_tables.visible_run_dataframe(
            run_snapshot_id=snapshot.run_snapshot_id,
            committed_runs=committed_runs,
        )
        build = build_distributed_gold(
            spark,
            visible_silver=visible,
            registry=registry,
            policy_context=context,
            field_policy=policy,
            committed_run_ids=committed_run_ids,
            committed_runs=committed_runs if epoch_input else None,
            silver_epoch_id=snapshot.epoch_id if epoch_input else None,
            committed_run_count=(snapshot.committed_run_count if epoch_input else None),
            committed_run_digest=(
                snapshot.committed_run_digest if epoch_input else None
            ),
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
            freshness_policy=freshness_policy,
            build_mode=build_mode,
            termination_fences=termination_fences,
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
            if build_mode == GoldBuildMode.CANDIDATE_BACKFILL:
                return {
                    "releasePlanId": build.plan.release_plan_id,
                    "buildMode": build_mode.value,
                    "qualityStatus": build.quality_report.status.value,
                    "qualityReport": quality_ref.model_dump(
                        mode="json",
                        by_alias=True,
                        exclude_none=True,
                    ),
                    "violations": build.quality_report.violations,
                    "committed": False,
                }
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
        commit_ref = store.upload_bytes(
            commit.json_bytes(),
            join_uri(plan_prefix, "release-commit.json"),
            media_type=GOLD_RELEASE_COMMIT_MEDIA_TYPE,
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=CONTROL_MAX_BYTES,
        ).object_ref
        return {
            "releasePlanId": build.plan.release_plan_id,
            "buildMode": build_mode.value,
            "qualityStatus": build.quality_report.status.value,
            "committed": True,
            "commitKey": commit.commit_key,
            "qualityReport": quality_ref.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
            "attributionManifest": attribution_ref.model_dump(
                mode="json", by_alias=True, exclude_none=True
            ),
            "releaseCommit": commit_ref.model_dump(
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
