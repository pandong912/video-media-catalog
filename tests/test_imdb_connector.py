from __future__ import annotations

import csv
import gzip

from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.connector import ConnectorRecordEnvelope
from video_media_catalog.imdb import (
    IMDB_DATASET_COLUMNS,
    IMDB_DATASET_FILES,
    imdb_rights_profile,
    iter_imdb_rows,
    map_imdb_record,
)
from video_media_catalog.imdb_sync import capture_imdb_snapshot
from video_media_catalog.object_store import BoundedObjectStore
from video_media_catalog.rights import UsageAction
from video_media_catalog.source_silver import build_source_silver_rows
from video_media_catalog.storage import local_path


def _write(path, dataset: str, values: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, mode="wt", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t", lineterminator="\n")
        writer.writerow(IMDB_DATASET_COLUMNS[dataset])
        writer.writerow(values)


def _datasets(tmp_path):
    rows = {
        "title.basics.tsv.gz": [
            "tt0000001",
            "movie",
            "Example",
            "Example Original",
            "0",
            "2020",
            r"\N",
            "120",
            "Drama,Mystery",
        ],
        "title.akas.tsv.gz": [
            "tt0000001",
            "1",
            "示例",
            "CN",
            "zh",
            "imdbDisplay",
            r"\N",
            "0",
        ],
        "title.episode.tsv.gz": ["tt0000002", "tt0000003", "1", "2"],
        "title.crew.tsv.gz": ["tt0000001", "nm0000001", "nm0000001"],
        "title.principals.tsv.gz": [
            "tt0000001",
            "1",
            "nm0000001",
            "actor",
            r"\N",
            '["Lead"]',
        ],
        "title.ratings.tsv.gz": ["tt0000001", "8.2", "1000"],
        "name.basics.tsv.gz": [
            "nm0000001",
            "Example Person",
            "1980",
            r"\N",
            "actor,director",
            "tt0000001",
        ],
    }
    paths = {}
    for dataset in IMDB_DATASET_FILES:
        path = tmp_path / dataset
        _write(path, dataset, rows[dataset])
        paths[dataset] = path
    return paths


def _records(result) -> list[ConnectorRecordEnvelope]:
    return [
        ConnectorRecordEnvelope.model_validate_json(line)
        for reference in result.record_set_manifest.record_objects
        for line in local_path(reference.uri).read_bytes().splitlines()
    ]


def test_imdb_tsv_treats_unmatched_quotes_as_literal_text(tmp_path) -> None:
    path = tmp_path / "title.basics.tsv.gz"
    header = "\t".join(IMDB_DATASET_COLUMNS["title.basics.tsv.gz"])
    row = (
        'tt10233364\ttvEpisode\t"Rolling in the Deep Dish\t'
        '"Rolling in the Deep Dish\t0\t2019\t\\N\t22\tReality-TV'
    )
    with gzip.open(path, mode="wt", encoding="utf-8", newline="") as handle:
        handle.write(f"{header}\n{row}\n")

    assert list(iter_imdb_rows(path, "title.basics.tsv.gz")) == [
        {
            "tconst": "tt10233364",
            "titleType": "tvEpisode",
            "primaryTitle": '"Rolling in the Deep Dish',
            "originalTitle": '"Rolling in the Deep Dish',
            "isAdult": "0",
            "startYear": "2019",
            "endYear": None,
            "runtimeMinutes": "22",
            "genres": "Reality-TV",
        }
    ]


def test_imdb_official_snapshot_is_replayable_and_maps_all_row_families(
    tmp_path,
) -> None:
    result = capture_imdb_snapshot(
        dataset_paths=_datasets(tmp_path / "inputs"),
        destination_prefix=(tmp_path / "output").as_uri(),
        acquired_at="2026-09-20T00:00:00Z",
        image_digest="sha256:" + ("a" * 64),
        config_digest="sha256:" + ("b" * 64),
        store=BoundedObjectStore(client=object()),
        record_shard_bytes=32 * 1024,
        window_start="2026-09-19T00:00:00Z",
        window_end="2026-09-20T00:00:00Z",
        cursor="imdb-snapshot-2026-09-20",
        watermark="2026-09-20",
    )

    assert result.batch_manifest.record_count == len(IMDB_DATASET_FILES)
    assert result.batch_manifest.delete_coverage.value == "SNAPSHOT_DIFF"
    assert result.batch_manifest.source_window is not None
    assert result.batch_manifest.watermark_after == "2026-09-20"
    assert result.batch_manifest.coverage_scope["cursor"] == (
        "imdb-snapshot-2026-09-20"
    )
    records = _records(result)
    mapped = [map_imdb_record(item) for item in records]
    assert {item.source_node.namespace_id for item in mapped} == {
        "imdb-title",
        "imdb-name",
    }
    assert {
        item.source_node.referent_kind
        for item in mapped
        if item.source_node.source_id == "tt0000001"
    } == {"EDITORIAL_WORK"}
    movie_identifier = next(
        item
        for item in mapped
        if item.source_node.source_id == "tt0000001" and item.identifier_assertions
    ).identifier_assertions[0]
    assert movie_identifier.referent_kind == "EDITORIAL_WORK"
    assert any(
        assertion.predicate == "cast_member"
        for item in mapped
        for assertion in item.relationship_assertions
    )
    run, rows = build_source_silver_rows(
        registry=build_community_registry(),
        batch=result.batch_manifest,
        record_set=result.record_set_manifest,
        envelopes=records,
    )
    assert run.source_product_id == "imdb-non-commercial-datasets"
    assert len(rows["community_source_record"]) == len(IMDB_DATASET_FILES)
    assert rows["community_relationship_assertion"]


def test_imdb_rights_are_owner_only_and_non_exportable() -> None:
    policy = imdb_rights_profile()
    assert policy.allows(
        UsageAction.SEARCH,
        audience="research",
        purpose="research",
    )
    assert not policy.allows(
        UsageAction.SEARCH,
        audience="internal",
        purpose="research",
    )
    assert UsageAction.EXPORT not in policy.permissions
    assert UsageAction.REDISTRIBUTE not in policy.permissions
