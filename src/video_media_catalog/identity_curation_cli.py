"""Control-plane CLI for immutable identity curation requests."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_iceberg import CommunityCatalogTables
from video_media_catalog.community_snapshot import (
    CONTROL_MAX_BYTES,
    CommunitySilverSnapshotSet,
)
from video_media_catalog.identity_curation import (
    IDENTITY_CURATION_MANIFEST_MEDIA_TYPE,
    IdentityCurationManifest,
    build_identity_curation_dataframes,
    build_identity_curation_manifest_ref,
)
from video_media_catalog.research_silver_cli import (
    _add_catalog_args,
    _add_control_object_args,
    _catalog_config,
    _control_object_ref,
    _load_run_state,
    _object_store,
    _read_model,
    _spark_session,
    _validate_object_uri,
    _verify_data_counts,
)
from video_media_catalog.v2_contracts import (
    parse_rfc3339,
    require_oidc_subject,
    require_rfc3339,
    require_sha256,
)


def _add_object_store_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION"))
    parser.add_argument("--s3-endpoint", default=os.environ.get("S3_ENDPOINT"))
    parser.add_argument(
        "--s3-path-style-access",
        action="store_true",
        default=os.environ.get("S3_PATH_STYLE", "").lower() in {"1", "true", "yes"},
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="video-media-catalog-identity-curation",
        description=(
            "Publish and apply snapshot-pinned immutable identity review manifests."
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    publication = commands.add_parser(
        "publish",
        help="validate and immutably publish one complete curation manifest",
    )
    publication.add_argument("--manifest-file", required=True, type=Path)
    publication.add_argument("--destination-uri", required=True)
    _add_object_store_args(publication)
    publication.set_defaults(stage_runner=_run_publish)

    application = commands.add_parser(
        "apply",
        help="apply one immutable review manifest through Silver commit-last",
    )
    _add_control_object_args(application, "review_manifest")
    application.add_argument("--operator-subject", required=True)
    application.add_argument("--image-digest", required=True)
    application.add_argument("--config-digest", required=True)
    application.add_argument("--committed-at", required=True)
    _add_catalog_args(
        application,
        app_name="media-catalog-identity-curation",
    )
    application.set_defaults(stage_runner=_run_apply)
    return parser


def _run_publish(parsed: argparse.Namespace) -> dict[str, Any]:
    source = parsed.manifest_file.expanduser().resolve(strict=True)
    if not source.is_file():
        raise ValueError("manifest-file must identify one regular file")
    size = source.stat().st_size
    if not 0 < size <= CONTROL_MAX_BYTES:
        raise ValueError("curation manifest must be between 1 byte and 16 MiB")
    manifest = IdentityCurationManifest.model_validate_json(source.read_bytes())
    destination = _validate_object_uri(
        parsed.destination_uri,
        label="curation manifest destination",
    )
    store = _object_store(
        parsed,
        local_only=urlsplit(destination).scheme == "file",
    )
    uploaded = store.upload_bytes(
        manifest.json_bytes(),
        destination,
        media_type=IDENTITY_CURATION_MANIFEST_MEDIA_TYPE,
        object_format="OBJECT_FORMAT_JSON",
        max_bytes=CONTROL_MAX_BYTES,
    )
    manifest_ref = build_identity_curation_manifest_ref(
        manifest,
        uploaded.object_ref,
    )
    return {
        "requestId": manifest.manifest_id,
        "status": "SUBMITTED",
        "manifest": manifest_ref.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
        "reused": uploaded.reused,
    }


def _selected_visible_silver(
    spark: Any,
    *,
    tables: CommunityCatalogTables,
    snapshot: CommunitySilverSnapshotSet,
) -> dict[str, Any]:
    visible = tables.visible_dataframes(
        data_snapshot_ids=snapshot.data_snapshot_ids,
        commit_snapshot_id=snapshot.commit_snapshot_id,
    )
    selected = spark.createDataFrame(
        [(run_id,) for run_id in snapshot.committed_run_ids],
        "run_id STRING",
    )
    return {
        table: frame.join(selected, "run_id", "inner")
        for table, frame in visible.items()
    }


def _run_apply(parsed: argparse.Namespace) -> dict[str, Any]:
    committed_at = require_rfc3339(parsed.committed_at, label="committed-at")
    operator_subject = require_oidc_subject(
        parsed.operator_subject,
        label="operator-subject",
    )
    image_digest = require_sha256(parsed.image_digest, label="image-digest")
    config_digest = require_sha256(parsed.config_digest, label="config-digest")
    reference = _control_object_ref(
        parsed,
        "review_manifest",
        media_type=IDENTITY_CURATION_MANIFEST_MEDIA_TYPE,
    )
    manifest_store = _object_store(
        parsed,
        local_only=urlsplit(reference.uri).scheme == "file",
    )
    manifest = _read_model(
        manifest_store,
        reference,
        IdentityCurationManifest,
    )
    if manifest.image_digest != image_digest or manifest.config_digest != config_digest:
        raise ValueError("runtime image/config digests differ from curation manifest")
    if manifest.operator_subject != operator_subject:
        raise ValueError("runtime operator subject differs from curation manifest")
    manifest_ref = build_identity_curation_manifest_ref(manifest, reference)
    snapshot_ref = manifest.pinned_silver_snapshot.object
    snapshot_store = _object_store(
        parsed,
        local_only=urlsplit(snapshot_ref.uri).scheme == "file",
    )
    snapshot = _read_model(
        snapshot_store,
        snapshot_ref,
        CommunitySilverSnapshotSet,
    )
    if snapshot.snapshot_set_id != manifest.pinned_silver_snapshot.snapshot_set_id:
        raise ValueError("curation manifest binds another Silver snapshot set")
    if parse_rfc3339(snapshot.created_at) > parse_rfc3339(manifest.operated_at):
        raise ValueError("curation operation cannot precede its pinned snapshot")
    if parse_rfc3339(committed_at) < parse_rfc3339(manifest.operated_at):
        raise ValueError("curation commit cannot precede the operation")

    config = _catalog_config(parsed)
    spark = _spark_session(parsed, config)
    frames: dict[str, Any] | None = None
    try:
        tables = CommunityCatalogTables(spark, config)
        _, commits = _load_run_state(
            spark,
            tables=tables,
            run_snapshot_id=snapshot.run_snapshot_id,
            commit_snapshot_id=snapshot.commit_snapshot_id,
            run_ids=snapshot.committed_run_ids,
        )
        _verify_data_counts(
            spark,
            tables=tables,
            run_ids=snapshot.committed_run_ids,
            commits=commits,
            data_snapshot_ids=snapshot.data_snapshot_ids,
        )
        visible = _selected_visible_silver(
            spark,
            tables=tables,
            snapshot=snapshot,
        )
        run, frames = build_identity_curation_dataframes(
            spark,
            visible_silver=visible,
            manifest=manifest,
            manifest_ref=manifest_ref,
        )
        commit = tables.stage_and_commit(
            run=run,
            dataframes=frames,
            committed_at=committed_at,
        )
        return {
            "requestId": manifest.manifest_id,
            "status": "APPLIED",
            "runId": run.run_id,
            "commitKey": commit.commit_key,
            "silverSnapshotSetId": snapshot.snapshot_set_id,
            "conflictKeys": manifest.conflict_keys,
            "decisionKeys": manifest.decision_keys,
            "tableCounts": commit.table_counts,
            "tableSnapshotIds": commit.table_snapshot_ids,
        }
    finally:
        if frames is not None:
            for frame in frames.values():
                frame.unpersist()
        spark.stop()


def run(parsed: argparse.Namespace) -> dict[str, Any]:
    return parsed.stage_runner(parsed)


def main(argv: Sequence[str] | None = None) -> int:
    print(canonical_json(run(build_parser().parse_args(argv))))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
