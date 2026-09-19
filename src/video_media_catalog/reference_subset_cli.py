"""Spark CLI for the content-first 100k Wikidata reference subset."""

from __future__ import annotations

import argparse
import hashlib
import tempfile
import uuid
from collections.abc import Sequence
from contextlib import suppress
from pathlib import Path
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.object_store import (
    BoundedObjectStore,
    S3Location,
    _etag,
)
from video_media_catalog.reference_selection import (
    DEFAULT_AGENT_LIMITS,
    DEFAULT_CONTENT_QUOTAS,
    REFERENCE_SELECTION_ALGORITHM_ID,
    AssetDemandProfile,
    ReferenceQualityThresholds,
    ReferenceSelectionConfig,
    ReferenceSubsetAuditManifest,
    reference_subset_build_digest,
)
from video_media_catalog.source_manifest import (
    SOURCE_MANIFEST_MEDIA_TYPE,
    write_source_manifest,
)
from video_media_catalog.wikidata_subset import subset_source_manifest_entry
from video_media_catalog.wikidata_subset_cli import (
    MAX_CONTROL_BYTES,
    _delete_temporary_prefix,
    _dump_reference,
    _join,
    _load_or_build_staging,
    _normalization_locations,
    _prefix,
    _publish_s3_part,
    _read_small_json,
    _require_versioned,
    _s3_client,
    _spark_session,
    _spark_uri,
    _temporary_part,
    _temporary_spark_locations,
)
from video_media_catalog.wikidata_subset_cli import (
    build_parser as legacy_build_parser,
)
from video_media_catalog.wikidata_subset_spark import (
    build_reference_subset,
    configure_bfs_materialize_dir,
)


def build_parser() -> argparse.ArgumentParser:
    parser = legacy_build_parser()
    parser.prog = "video-media-catalog-reference-subset"
    parser.description = (
        "Build the content-first, quality-gated Wikidata reference subset "
        "from one immutable normalized dump."
    )
    parser.set_defaults(
        target_count=sum(DEFAULT_CONTENT_QUOTAS.values()),
        movie_count=DEFAULT_CONTENT_QUOTAS["MOVIE"],
        tv_series_count=DEFAULT_CONTENT_QUOTAS["TV_SERIES"],
        tv_season_count=DEFAULT_CONTENT_QUOTAS["TV_SEASON"],
        tv_episode_count=DEFAULT_CONTENT_QUOTAS["TV_EPISODE"],
        app_name="video-media-catalog-reference-subset",
    )
    parser.add_argument(
        "--person-limit",
        type=int,
        default=DEFAULT_AGENT_LIMITS["PERSON"],
    )
    parser.add_argument(
        "--organization-limit",
        type=int,
        default=DEFAULT_AGENT_LIMITS["ORGANIZATION"],
    )
    parser.add_argument("--demand-profile-uri")
    parser.add_argument("--demand-profile-sha256")
    parser.add_argument("--demand-profile-size", type=int)
    parser.add_argument("--demand-profile-version")
    parser.add_argument("--demand-profile-etag")
    parser.add_argument(
        "--minimum-title-coverage",
        type=float,
        default=ReferenceQualityThresholds().minimum_title_coverage,
    )
    parser.add_argument(
        "--minimum-movie-release-coverage",
        type=float,
        default=ReferenceQualityThresholds().minimum_movie_release_coverage,
    )
    parser.add_argument(
        "--minimum-episode-parent-coverage",
        type=float,
        default=ReferenceQualityThresholds().minimum_episode_parent_coverage,
    )
    return parser


def _read_demand_profile(
    parsed: argparse.Namespace,
    *,
    s3: Any,
    store: BoundedObjectStore,
) -> tuple[AssetDemandProfile | None, ObjectRef | None]:
    values = (
        parsed.demand_profile_uri,
        parsed.demand_profile_sha256,
        parsed.demand_profile_size,
        parsed.demand_profile_version,
        parsed.demand_profile_etag,
    )
    if not any(value is not None for value in values):
        return None, None
    if not all(value is not None for value in values):
        raise ValueError("all demand-profile object arguments must be provided")
    location = S3Location.parse(str(parsed.demand_profile_uri))
    etag = _etag(str(parsed.demand_profile_etag))
    version = str(parsed.demand_profile_version).strip()
    if etag is None or not version or version == "null":
        raise ValueError("demand profile ETag and VersionId must be non-empty")
    reference = ObjectRef(
        uri=location.uri,
        format="OBJECT_FORMAT_JSON",
        media_type="application/json",
        checksum=Checksum(value=str(parsed.demand_profile_sha256)),
        size_bytes=int(parsed.demand_profile_size),
        etag=etag,
        object_version=version,
    )
    store.verify(reference, max_bytes=MAX_CONTROL_BYTES)
    response = s3.get_object(
        Bucket=location.bucket,
        Key=location.key,
        VersionId=version,
        ChecksumMode="ENABLED",
    )
    body = response.get("Body")
    if body is None or not hasattr(body, "read"):
        raise RuntimeError("demand profile object has no readable body")
    try:
        payload = body.read(MAX_CONTROL_BYTES + 1)
    finally:
        body.close()
    if (
        len(payload) != reference.size_bytes
        or len(payload) > MAX_CONTROL_BYTES
        or hashlib.sha256(payload).hexdigest() != reference.checksum.value
    ):
        raise ValueError("demand profile content does not match ObjectRef")
    return AssetDemandProfile.model_validate_json(payload), reference


def _existing_audit(
    s3: Any,
    location: S3Location,
    *,
    dump: ObjectRef,
    demand_profile: ObjectRef | None,
    config: ReferenceSelectionConfig,
    thresholds: ReferenceQualityThresholds,
) -> ReferenceSubsetAuditManifest | None:
    payload = _read_small_json(s3, location)
    if payload is None:
        return None
    audit = ReferenceSubsetAuditManifest.model_validate_json(payload)
    if (
        audit.dump != dump
        or audit.demand_profile != demand_profile
        or audit.config_digest != config.digest
        or audit.quality_thresholds_digest != thresholds.digest
        or audit.build_digest != reference_subset_build_digest(config, thresholds)
    ):
        raise ValueError("existing reference audit conflicts with requested build")
    return audit


def run(
    parsed: argparse.Namespace,
    *,
    s3: Any | None = None,
    spark: Any | None = None,
) -> ReferenceSubsetAuditManifest:
    if parsed.max_dump_bytes < 1:
        raise ValueError("max-dump-bytes must be positive")
    if parsed.max_output_bytes < 1 or parsed.max_output_bytes > 4 * 1024**3:
        raise ValueError("max-output-bytes must be between 1 and 4 GiB")
    if parsed.max_closure_iterations < 1:
        raise ValueError("max-closure-iterations must be positive")
    if parsed.shuffle_partitions is not None and parsed.shuffle_partitions < 1:
        raise ValueError("shuffle-partitions must be positive")

    content_quotas = {
        "MOVIE": parsed.movie_count,
        "TV_SERIES": parsed.tv_series_count,
        "TV_SEASON": parsed.tv_season_count,
        "TV_EPISODE": parsed.tv_episode_count,
    }
    if parsed.target_count != sum(content_quotas.values()):
        raise ValueError("target-count must equal the four content quotas")
    thresholds = ReferenceQualityThresholds(
        minimum_title_coverage=parsed.minimum_title_coverage,
        minimum_movie_release_coverage=(parsed.minimum_movie_release_coverage),
        minimum_episode_parent_coverage=(parsed.minimum_episode_parent_coverage),
    )
    client = s3 or _s3_client(parsed)
    store = BoundedObjectStore(
        region=parsed.aws_region,
        endpoint_url=parsed.s3_endpoint,
        path_style_access=parsed.s3_path_style_access,
        client=client,
    )
    profile, profile_ref = _read_demand_profile(
        parsed,
        s3=client,
        store=store,
    )
    config = ReferenceSelectionConfig(
        content_quotas=content_quotas,
        agent_limits={
            "PERSON": parsed.person_limit,
            "ORGANIZATION": parsed.organization_limit,
        },
        demand_profile_digest=None if profile is None else profile.digest,
    )
    build_digest = reference_subset_build_digest(config, thresholds)
    dump, dump_date = _dump_reference(parsed, s3=client, store=store)
    output_prefix = _prefix(parsed.output_prefix)
    staging_prefix = _prefix(parsed.staging_prefix)
    build_root = _join(
        output_prefix,
        f"date={dump_date}",
        f"dump-sha256={dump.checksum.value}",
        f"reference-build-sha256={build_digest.removeprefix('sha256:')}",
    )
    audit_location = _join(build_root, "audit-manifest.json")
    existing = _existing_audit(
        client,
        audit_location,
        dump=dump,
        demand_profile=profile_ref,
        config=config,
        thresholds=thresholds,
    )
    if existing is not None:
        store.verify(existing.subset, max_bytes=parsed.max_output_bytes)
        store.verify(existing.source_manifest, max_bytes=MAX_CONTROL_BYTES)
        return existing

    data_location, marker_location, staging_uri = _normalization_locations(
        staging_prefix,
        dump,
    )
    owns_spark = spark is None
    session = spark or _spark_session(parsed)
    temporary = _join(
        output_prefix,
        "_temporary",
        f"date={dump_date}",
        f"reference-build-sha256={build_digest.removeprefix('sha256:')}",
        f"spark-output-{uuid.uuid4().hex}",
    )
    temporary_output, bfs_scratch_uri = _temporary_spark_locations(temporary)
    configure_bfs_materialize_dir(session, bfs_scratch_uri)
    try:
        normalized = _load_or_build_staging(
            spark=session,
            s3=client,
            store=store,
            dump=dump,
            data=data_location,
            marker=marker_location,
        )
        built = build_reference_subset(
            session,
            normalized,
            config,
            demand_profile=profile,
            quality_thresholds=thresholds,
            max_closure_iterations=parsed.max_closure_iterations,
        )
        (
            built.lines.write.mode("errorifexists")
            .option("compression", "bzip2")
            .text(_spark_uri(temporary_output.uri))
        )
        store.verify(dump, max_bytes=parsed.max_dump_bytes)
        part, part_version = _temporary_part(client, temporary_output)
        subset = _publish_s3_part(
            s3=client,
            source=part,
            source_version=part_version,
            destination=_join(
                build_root,
                f"wikidata-{dump_date}-reference-subset.json.bz2",
            ),
            config_digest=build_digest,
            dump_sha256=dump.checksum.value,
            max_bytes=parsed.max_output_bytes,
            algorithm_id=REFERENCE_SELECTION_ALGORITHM_ID,
        )
        _require_versioned(subset, "reference subset")

        manifest_location = _join(build_root, "source-manifest.parquet")
        with tempfile.TemporaryDirectory(
            prefix="reference-catalog-source-manifest-",
        ) as directory:
            manifest_path = Path(directory) / "source-manifest.parquet"
            write_source_manifest(
                manifest_path,
                [subset_source_manifest_entry(subset)],
            )
            source_manifest = store.upload_file(
                manifest_path,
                manifest_location.uri,
                media_type=SOURCE_MANIFEST_MEDIA_TYPE,
                object_format="OBJECT_FORMAT_PARQUET",
                max_bytes=MAX_CONTROL_BYTES,
            ).object_ref
        _require_versioned(source_manifest, "source manifest")

        audit = ReferenceSubsetAuditManifest(
            config_digest=config.digest,
            quality_thresholds_digest=thresholds.digest,
            build_digest=build_digest,
            demand_profile_digest=config.demand_profile_digest,
            demand_profile=profile_ref,
            dump=dump,
            subset=subset,
            source_manifest=source_manifest,
            content_quotas=config.content_quotas,
            agent_limits=config.agent_limits,
            quality_thresholds=thresholds,
            selected_count=len(built.selection.selected_qids),
            dependency_rows=built.dependency_rows,
            output_rows=built.output_rows,
            pruned_relation_statements=built.pruned_relation_statements,
            normalization_staging_uri=staging_uri,
            quality=built.quality,
        )
        audit_ref = store.upload_bytes(
            audit.json_bytes(),
            audit_location.uri,
            media_type="application/json",
            object_format="OBJECT_FORMAT_JSON",
            max_bytes=MAX_CONTROL_BYTES,
        ).object_ref
        _require_versioned(audit_ref, "reference audit manifest")
        return audit
    finally:
        with suppress(Exception):
            _delete_temporary_prefix(client, temporary)
        if owns_spark:
            session.stop()


def main(argv: Sequence[str] | None = None) -> int:
    audit = run(build_parser().parse_args(argv))
    print(
        canonical_json(audit.model_dump(mode="json", by_alias=True, exclude_none=True))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
