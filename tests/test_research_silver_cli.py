from __future__ import annotations

from types import SimpleNamespace

import pytest

from video_media_catalog.community_ingest import (
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    SOURCE_TABLES,
    build_community_table_mapping,
)
from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.research_silver_cli import (
    MAX_EXPLICIT_RUN_IDS,
    _control_object_ref,
    _models_by_run,
    _normalize_run_ids,
    _optional_control_object_ref,
    _parse_source_watermarks,
    _require_immutable_snapshot_output,
    _selected_silver_frames,
    _validate_run_generations,
    _validate_source_watermark_changes,
    build_parser,
)


def _identity_arguments() -> list[str]:
    return [
        "resolve-identity",
        "--silver-snapshot-uri",
        "file:///tmp/silver-snapshot.json",
        "--silver-snapshot-hash",
        "b" * 64,
        "--silver-snapshot-size",
        "100",
        "--source-run-id",
        "sha256:" + ("c" * 64),
        "--identity-generation-id",
        "catalog-2026-09",
        "--identity-mode",
        "full",
        "--image-digest",
        "sha256:" + ("d" * 64),
        "--config-digest",
        "sha256:" + ("e" * 64),
        "--started-at",
        "2026-09-20T00:00:00Z",
        "--committed-at",
        "2026-09-20T00:01:00Z",
        "--catalog-type",
        "hadoop",
        "--warehouse",
        "file:///tmp/warehouse",
    ]


def test_parser_exposes_all_research_stages_with_existing_namespace() -> None:
    identity = build_parser().parse_args(
        [*_identity_arguments(), "--s3-credentials-provider", "default"]
    )
    assert identity.command == "resolve-identity"
    assert identity.namespace == "video_media_catalog"
    assert identity.source_run_ids == ["sha256:" + ("c" * 64)]
    assert identity.identity_generation_id == "catalog-2026-09"
    assert identity.identity_mode == "full"
    assert identity.s3_credentials_provider == "default"
    assert identity.silver_snapshot_media_type.endswith("silver-snapshot-set.v2+json")
    assert identity.identity_max_label_iterations == 64
    assert identity.identity_max_component_size == 256
    assert identity.identity_max_node_candidate_keys == 256
    assert identity.identity_max_component_candidate_keys == 256

    publication = build_parser().parse_args(
        [
            "publish-snapshot",
            "--run-id",
            "sha256:" + ("f" * 64),
            "--snapshot-uri",
            "file:///tmp/gold-input.json",
            "--created-at",
            "2026-09-20T00:02:00Z",
            "--catalog-type",
            "hadoop",
            "--warehouse",
            "file:///tmp/warehouse",
        ]
    )
    assert publication.command == "publish-snapshot"
    assert publication.namespace == "video_media_catalog"
    assert publication.identity_generation_id is None

    epoch = build_parser().parse_args(
        [
            "publish-epoch",
            "--delta-run-id",
            "sha256:" + ("f" * 64),
            "--source-watermark",
            "tvmaze-public-api=since:20",
            "--epoch-uri",
            "file:///tmp/silver-epoch.json",
            "--created-at",
            "2026-09-20T00:02:00Z",
            "--catalog-type",
            "hadoop",
            "--warehouse",
            "file:///tmp/warehouse",
        ]
    )
    assert epoch.command == "publish-epoch"
    assert epoch.parent_epoch_uri is None
    assert epoch.delta_run_ids == ["sha256:" + ("f" * 64)]
    assert epoch.identity_generation_id is None

    generated_epoch = build_parser().parse_args(
        [
            "publish-epoch",
            "--identity-generation-id",
            "catalog-2026-09",
            "--epoch-uri",
            "file:///tmp/generated-epoch.json",
            "--created-at",
            "2026-09-20T00:02:00Z",
            "--catalog-type",
            "hadoop",
            "--warehouse",
            "file:///tmp/warehouse",
        ]
    )
    assert generated_epoch.identity_generation_id == "catalog-2026-09"


def test_control_object_requires_pinned_s3_version_and_etag() -> None:
    arguments = _identity_arguments()
    arguments[2] = "s3://bucket/silver-snapshot.json"
    parsed = build_parser().parse_args(arguments)
    with pytest.raises(ValueError, match="requires version and ETag"):
        _control_object_ref(
            parsed,
            "silver_snapshot",
            media_type="application/vnd.example+json",
        )


def test_control_object_rejects_local_s3_metadata() -> None:
    arguments = [
        *_identity_arguments(),
        "--silver-snapshot-version",
        "version-1",
        "--silver-snapshot-etag",
        "etag-1",
    ]
    parsed = build_parser().parse_args(arguments)
    with pytest.raises(ValueError, match="file object cannot declare"):
        _control_object_ref(
            parsed,
            "silver_snapshot",
            media_type="application/vnd.example+json",
        )


def test_run_selection_is_bounded_unique_and_canonical() -> None:
    first = "sha256:" + ("a" * 64)
    second = "sha256:" + ("b" * 64)
    assert _normalize_run_ids(
        (second, first),
        label="test",
    ) == (first, second)
    with pytest.raises(ValueError, match="duplicate"):
        _normalize_run_ids((first, first), label="test")
    with pytest.raises(ValueError, match="at most"):
        _normalize_run_ids(
            tuple(first for _ in range(MAX_EXPLICIT_RUN_IDS + 1)),
            label="test",
        )


def test_pinned_run_loader_rejects_missing_manifest() -> None:
    counts = {table: 0 for table in DATA_TABLE_COLUMNS}
    run = build_community_ingest_run(
        run_kind=IngestRunKind.SOURCE_ASSERTIONS,
        source_product_id="tvmaze-public-api",
        input_id="sha256:" + ("1" * 64),
        policy_id="tvmaze-api-cc-by-sa",
        policy_digest="sha256:" + ("2" * 64),
        image_digest="sha256:" + ("3" * 64),
        config_digest="sha256:" + ("4" * 64),
        started_at="2026-09-20T00:00:00Z",
        expected_counts=counts,
        input_manifest={"recordSetId": "sha256:" + ("1" * 64)},
    )
    rows = [{"run_id": run.run_id, "manifest_json": run.json_bytes()}]
    assert (
        _models_by_run(
            rows,
            requested_run_ids=(run.run_id,),
            json_column="manifest_json",
            model=type(run),
            label="test",
        )[run.run_id]
        == run
    )
    with pytest.raises(ValueError, match="missing requested runs"):
        _models_by_run(
            rows,
            requested_run_ids=(run.run_id, "sha256:" + ("f" * 64)),
            json_column="manifest_json",
            model=type(run),
            label="test",
        )


def test_s3_snapshot_output_requires_version_and_etag() -> None:
    reference = ObjectRef(
        uri="s3://bucket/silver-snapshot.json",
        format="OBJECT_FORMAT_JSON",
        media_type="application/vnd.example+json",
        checksum=Checksum(value="a" * 64),
        size_bytes=100,
    )
    with pytest.raises(RuntimeError, match="bucket versioning"):
        _require_immutable_snapshot_output(reference)


def test_epoch_parent_object_ref_is_all_or_none() -> None:
    parsed = build_parser().parse_args(
        [
            "publish-epoch",
            "--epoch-uri",
            "file:///tmp/silver-epoch.json",
            "--created-at",
            "2026-09-20T00:02:00Z",
            "--parent-epoch-uri",
            "file:///tmp/parent.json",
            "--catalog-type",
            "hadoop",
            "--warehouse",
            "file:///tmp/warehouse",
        ]
    )
    with pytest.raises(ValueError, match="provided together"):
        _optional_control_object_ref(
            parsed,
            "parent_epoch",
            media_type="application/vnd.example+json",
        )


def test_source_watermarks_are_unique_and_canonical() -> None:
    assert _parse_source_watermarks(
        ("tvmaze-public-api=since:20", "tmdb-research=2026-09-20")
    ) == {
        "tmdb-research": "2026-09-20",
        "tvmaze-public-api": "since:20",
    }
    with pytest.raises(ValueError, match="duplicate"):
        _parse_source_watermarks(
            ("tvmaze-public-api=since:20", "tvmaze-public-api=since:21")
        )


def test_parent_watermark_change_requires_source_delta_run() -> None:
    parent = SimpleNamespace(source_watermarks={"tvmaze-public-api": "since:20"})
    with pytest.raises(ValueError, match="without a source delta"):
        _validate_source_watermark_changes(
            parent=parent,
            source_watermarks={"tvmaze-public-api": "since:21"},
            delta_runs={},
        )
    _validate_source_watermark_changes(
        parent=parent,
        source_watermarks={"tvmaze-public-api": "since:21"},
        delta_runs={
            "run": SimpleNamespace(
                source_product_id="tvmaze-public-api",
                run_kind=IngestRunKind.SOURCE_ASSERTIONS,
            )
        },
    )


def _identity_run(generation: str):
    mapping = build_community_table_mapping(generation)
    return build_community_ingest_run(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        source_product_id="identity-resolution-v2",
        input_id="sha256:" + ("1" * 64),
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("2" * 64),
        image_digest="sha256:" + ("3" * 64),
        config_digest="sha256:" + ("4" * 64),
        started_at="2026-09-20T00:00:00Z",
        expected_counts={table: 0 for table in DATA_TABLE_COLUMNS},
        input_manifest={
            "identityGenerationId": generation,
            "identityMode": "incremental",
            "tableMapping": mapping,
        },
    )


def test_incremental_run_generation_matches_and_cross_generation_fails() -> None:
    generation = "catalog-2026-09"
    run = _identity_run(generation)
    mapping = build_community_table_mapping(generation)
    _validate_run_generations(
        {run.run_id: run},
        identity_generation_id=generation,
        table_mapping=mapping,
    )

    with pytest.raises(ValueError, match="another Identity generation"):
        _validate_run_generations(
            {run.run_id: run},
            identity_generation_id="catalog-2026-10",
            table_mapping=build_community_table_mapping("catalog-2026-10"),
        )


def test_full_selection_does_not_read_historical_identity_tables() -> None:
    class RecordingVisibleTables:
        def __init__(self) -> None:
            self.selected_tables = None

        def visible_dataframes(self, **values):
            self.selected_tables = values["selected_tables"]
            return {}

    tables = RecordingVisibleTables()
    snapshot = SimpleNamespace(
        data_snapshot_ids={table: None for table in DATA_TABLE_COLUMNS},
        commit_snapshot_id=1,
    )
    _selected_silver_frames(
        tables=tables,
        snapshot=snapshot,
        committed_runs=object(),
        source_run_ids=("sha256:" + ("a" * 64),),
        source_only=True,
    )

    assert set(tables.selected_tables) == set(SOURCE_TABLES)


def test_generation_and_mode_participate_in_identity_run_id() -> None:
    first = _identity_run("catalog-2026-09")
    second = _identity_run("catalog-2026-10")
    payload = first.model_dump(mode="python")
    payload["input_manifest"] = {
        **first.input_manifest,
        "identityMode": "full",
    }
    full = build_community_ingest_run(
        **{
            key: value
            for key, value in payload.items()
            if key not in {"run_id", "schema_version"}
        }
    )

    assert first.run_id != second.run_id
    assert first.run_id != full.run_id
