from __future__ import annotations

import hashlib
from types import SimpleNamespace

import pytest

from video_media_catalog import research_silver_cli as silver_cli
from video_media_catalog.community_ingest import (
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_snapshot import (
    SILVER_SNAPSHOT_MEDIA_TYPE,
    build_community_silver_snapshot_set,
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
    _incremental_unprocessed_source_run_ids,
    _models_by_run,
    _normalize_run_ids,
    _optional_control_object_ref,
    _parentless_identity_lineage_run_ids,
    _parse_source_watermarks,
    _require_closed_identity_lineage,
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
    assert publication.initialize_identity_generation is False

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


def test_parentless_epoch_closes_incremental_identity_lineage_only() -> None:
    generation = "catalog-2026-09"
    mapping = build_community_table_mapping(generation)
    counts = {table: 0 for table in DATA_TABLE_COLUMNS}

    source = build_community_ingest_run(
        run_kind=IngestRunKind.SOURCE_ASSERTIONS,
        source_product_id="tmdb-research",
        input_id="sha256:" + ("1" * 64),
        policy_id="research-source",
        policy_digest="sha256:" + ("2" * 64),
        image_digest="sha256:" + ("3" * 64),
        config_digest="sha256:" + ("4" * 64),
        started_at="2026-09-20T00:00:00Z",
        expected_counts=counts,
        input_manifest={"captureId": "source"},
    )
    parent_identity = build_community_ingest_run(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        source_product_id="identity-resolution-v2",
        input_id="sha256:" + ("5" * 64),
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("6" * 64),
        image_digest="sha256:" + ("7" * 64),
        config_digest="sha256:" + ("8" * 64),
        started_at="2026-09-20T01:00:00Z",
        expected_counts=counts,
        input_manifest={
            "identityGenerationId": generation,
            "identityMode": "full",
            "tableMapping": mapping,
        },
    )
    parent_snapshot = build_community_silver_snapshot_set(
        committed_run_ids=(source.run_id, parent_identity.run_id),
        run_snapshot_id=11,
        commit_snapshot_id=12,
        data_snapshot_ids={table: None for table in DATA_TABLE_COLUMNS},
        identity_generation_id=generation,
        table_mapping=mapping,
        created_at="2026-09-20T02:00:00Z",
    )
    parent_ref = ObjectRef(
        uri="file:///tmp/identity-parent-snapshot.json",
        format="OBJECT_FORMAT_JSON",
        media_type=SILVER_SNAPSHOT_MEDIA_TYPE,
        checksum=Checksum(value="9" * 64),
        size_bytes=100,
    )
    incremental_identity = build_community_ingest_run(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        source_product_id="identity-resolution-v2",
        input_id="sha256:" + ("a" * 64),
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("b" * 64),
        image_digest="sha256:" + ("c" * 64),
        config_digest="sha256:" + ("d" * 64),
        started_at="2026-09-20T03:00:00Z",
        expected_counts=counts,
        input_manifest={
            "identityGenerationId": generation,
            "identityMode": "incremental",
            "tableMapping": mapping,
            "pinnedInputs": {
                "identityGenerationId": generation,
                "identityMode": "incremental",
                "silverSnapshot": {
                    "object": parent_ref.model_dump(
                        mode="json",
                        by_alias=True,
                        exclude_none=True,
                    ),
                    "snapshotSetId": parent_snapshot.snapshot_set_id,
                },
                "sourceRunIds": [source.run_id],
                "tableMapping": mapping,
            },
        },
    )
    concurrent_identity = build_community_ingest_run(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        source_product_id="identity-resolution-v2",
        input_id="sha256:" + ("e" * 64),
        policy_id="internal-key-continuity",
        policy_digest="sha256:" + ("f" * 64),
        image_digest="sha256:" + ("0" * 64),
        config_digest="sha256:" + ("1" * 64),
        started_at="2026-09-20T04:00:00Z",
        expected_counts=counts,
        input_manifest={
            "identityGenerationId": generation,
            "identityMode": "full",
            "tableMapping": mapping,
        },
    )
    available = {
        source.run_id: source,
        parent_identity.run_id: parent_identity,
        concurrent_identity.run_id: concurrent_identity,
    }

    lineage = _parentless_identity_lineage_run_ids(
        {
            source.run_id: source,
            incremental_identity.run_id: incremental_identity,
        },
        identity_generation_id=generation,
        table_mapping=mapping,
        read_manifest=lambda reference: parent_snapshot,
        load_runs=lambda manifest, run_ids: {
            run_id: available[run_id] for run_id in run_ids
        },
    )

    assert lineage == (parent_identity.run_id,)
    assert concurrent_identity.run_id not in lineage
    with pytest.raises(ValueError, match="omits pinned Identity ancestors"):
        _require_closed_identity_lineage(
            (source.run_id, incremental_identity.run_id),
            lineage,
        )
    _require_closed_identity_lineage(
        (source.run_id, parent_identity.run_id, incremental_identity.run_id),
        lineage,
    )


@pytest.mark.parametrize("initialize_generation", [False, True])
def test_snapshot_publication_closes_requested_identity_lineage(
    monkeypatch: pytest.MonkeyPatch,
    initialize_generation: bool,
) -> None:
    generation = "catalog-2026-09"
    requested_run_id = "sha256:" + ("1" * 64)
    ancestor_run_id = "sha256:" + ("2" * 64)
    mapping = build_community_table_mapping(generation)
    load_calls: list[tuple[str, ...]] = []
    initialization_calls: list[str] = []

    class FakeSpark:
        def stop(self) -> None:
            pass

    class FakeTables:
        def __init__(self, *_args, **_kwargs) -> None:
            self.table_mapping = mapping

        def latest_snapshot_id(self, _table: str) -> int:
            return 7

        def create_tables(self) -> None:
            initialization_calls.append("create")

        def assert_identity_tables_empty(self) -> None:
            initialization_calls.append("assert-empty")

    class FakeStore:
        def upload_bytes(
            self,
            body: bytes,
            uri: str,
            *,
            media_type: str,
            object_format: str,
            max_bytes: int,
        ) -> SimpleNamespace:
            assert object_format == "OBJECT_FORMAT_JSON"
            assert max_bytes > len(body)
            return SimpleNamespace(
                object_ref=ObjectRef(
                    uri=uri,
                    format=object_format,
                    media_type=media_type,
                    checksum=Checksum(value=hashlib.sha256(body).hexdigest()),
                    size_bytes=len(body),
                ),
                reused=False,
            )

        def verify(self, _reference: ObjectRef, *, max_bytes: int) -> None:
            assert max_bytes > 0

    def fake_load_run_state(*_args, run_ids, **_kwargs):
        normalized = tuple(run_ids)
        load_calls.append(normalized)
        return (
            {
                run_id: SimpleNamespace(
                    run_id=run_id,
                    run_kind=IngestRunKind.IDENTITY_RESOLUTION,
                )
                for run_id in normalized
            },
            {run_id: SimpleNamespace(run_id=run_id) for run_id in normalized},
        )

    monkeypatch.setattr(silver_cli, "_spark_session", lambda *_args: FakeSpark())
    monkeypatch.setattr(silver_cli, "CommunityCatalogTables", FakeTables)
    monkeypatch.setattr(silver_cli, "_load_run_state", fake_load_run_state)
    monkeypatch.setattr(
        silver_cli,
        "_validate_run_generations",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        silver_cli,
        "_publication_identity_lineage_run_ids",
        lambda *_args, **_kwargs: (ancestor_run_id,),
    )
    monkeypatch.setattr(
        silver_cli,
        "_verify_data_counts",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        silver_cli,
        "_incremental_unprocessed_source_run_ids",
        lambda _runs: (requested_run_id,),
    )
    monkeypatch.setattr(
        silver_cli,
        "_object_store",
        lambda *_args, **_kwargs: FakeStore(),
    )

    arguments = [
        "publish-snapshot",
        "--run-id",
        requested_run_id,
        "--snapshot-uri",
        "file:///tmp/lineage-closed-snapshot.json",
        "--created-at",
        "2026-09-20T00:02:00Z",
        "--identity-generation-id",
        generation,
        "--catalog-type",
        "hadoop",
        "--warehouse",
        "file:///tmp/warehouse",
    ]
    if initialize_generation:
        arguments.append("--initialize-identity-generation")
    parsed = build_parser().parse_args(arguments)
    summary = silver_cli._run_snapshot_publication(parsed)

    assert load_calls == [
        (requested_run_id,),
        tuple(sorted((requested_run_id, ancestor_run_id))),
    ]
    assert summary["requestedRunIds"] == (requested_run_id,)
    assert summary["identityLineageRunIds"] == (ancestor_run_id,)
    assert summary["unprocessedIdentitySourceRunIds"] == (requested_run_id,)
    assert summary["identityGenerationInitialized"] is initialize_generation
    assert initialization_calls == (
        ["create", "assert-empty"] if initialize_generation else []
    )
    assert summary["committedRunIds"] == tuple(
        sorted((requested_run_id, ancestor_run_id))
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


def test_incremental_identity_selects_only_unrepresented_source_runs() -> None:
    first = "sha256:" + ("a" * 64)
    second = "sha256:" + ("b" * 64)
    identity = "sha256:" + ("c" * 64)
    runs = {
        first: SimpleNamespace(
            run_kind=IngestRunKind.SOURCE_ASSERTIONS,
            input_manifest={},
        ),
        second: SimpleNamespace(
            run_kind=IngestRunKind.SOURCE_ASSERTIONS,
            input_manifest={},
        ),
        identity: SimpleNamespace(
            run_kind=IngestRunKind.IDENTITY_RESOLUTION,
            input_manifest={
                "pinnedInputs": {
                    "sourceRunIds": [first],
                }
            },
        ),
    }

    assert _incremental_unprocessed_source_run_ids(runs) == (second,)


def test_incremental_identity_counts_transitive_ancestor_source_coverage() -> None:
    ancestor_source = "sha256:" + ("a" * 64)
    head_source = "sha256:" + ("b" * 64)
    unprocessed_source = "sha256:" + ("c" * 64)
    runs = {
        run_id: SimpleNamespace(
            run_kind=IngestRunKind.SOURCE_ASSERTIONS,
            input_manifest={},
        )
        for run_id in (ancestor_source, head_source, unprocessed_source)
    }
    runs["ancestor"] = SimpleNamespace(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        input_manifest={"pinnedInputs": {"sourceRunIds": [ancestor_source]}},
    )
    runs["head"] = SimpleNamespace(
        run_kind=IngestRunKind.IDENTITY_RESOLUTION,
        input_manifest={"pinnedInputs": {"sourceRunIds": [head_source]}},
    )

    assert _incremental_unprocessed_source_run_ids(runs) == (unprocessed_source,)


def test_incremental_identity_rejects_unpinned_baseline_identity() -> None:
    source = "sha256:" + ("a" * 64)
    identity = "sha256:" + ("b" * 64)
    runs = {
        source: SimpleNamespace(
            run_kind=IngestRunKind.SOURCE_ASSERTIONS,
            input_manifest={},
        ),
        identity: SimpleNamespace(
            run_kind=IngestRunKind.IDENTITY_RESOLUTION,
            input_manifest={},
        ),
    }

    with pytest.raises(ValueError, match="lacks pinned sourceRunIds"):
        _incremental_unprocessed_source_run_ids(runs)


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
