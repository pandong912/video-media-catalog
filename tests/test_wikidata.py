from __future__ import annotations

import bz2
import gzip
from pathlib import Path

import pytest

from video_media_catalog.canonical import source_hash
from video_media_catalog.constants import RELEVANT_WIKIDATA_PROPERTIES
from video_media_catalog.wikidata import (
    iter_wikidata_entities,
    iter_wikidata_records,
    normalize_wikidata_entity,
)


def test_plain_gzip_and_bzip2_have_identical_entities(
    fixture_dir: Path, tmp_path: Path
) -> None:
    plain = fixture_dir / "wikidata.json"
    payload = plain.read_bytes()
    gzip_path = tmp_path / "wikidata.json.gz"
    bzip_path = tmp_path / "wikidata.json.bz2"
    gzip_path.write_bytes(gzip.compress(payload, mtime=0))
    bzip_path.write_bytes(bz2.compress(payload))

    expected = [entity["id"] for entity in iter_wikidata_entities(plain)]
    assert len(expected) == 14
    assert [entity["id"] for entity in iter_wikidata_entities(gzip_path)] == expected
    assert [entity["id"] for entity in iter_wikidata_entities(bzip_path)] == expected


def test_preserves_multilingual_values_sitelinks_claim_rank_and_p1545(
    fixture_dir: Path,
) -> None:
    movie = next(
        entity
        for entity in iter_wikidata_entities(fixture_dir / "wikidata.json")
        if entity["id"] == "Q1001"
    )
    normalized = normalize_wikidata_entity(movie)

    assert normalized["labels"]["zh"]["value"] == "示例电影"
    assert normalized["aliases"]["en"][0]["value"] == "The Example Film"
    assert normalized["descriptions"]["en"]["value"] == "fixture movie"
    assert normalized["sitelinks"]["enwiki"]["title"] == "Example Movie"
    assert set(normalized["claims"]) <= RELEVANT_WIKIDATA_PROPERTIES
    director = normalized["claims"]["P57"][0]
    assert director["rank"] == "preferred"
    assert director["qualifiers"]["P1545"][0]["datavalue"]["value"] == "1"


def test_wikidata_landing_record_is_stable(fixture_dir: Path) -> None:
    path = fixture_dir / "wikidata.json"
    first = list(iter_wikidata_records(path))
    second = list(iter_wikidata_records(path))

    assert first == second
    assert first[5].source_hash == source_hash(
        normalize_wikidata_entity(
            list(iter_wikidata_entities(path))[5],
        )
    )


def test_reports_line_number_for_non_line_oriented_json(tmp_path: Path) -> None:
    path = tmp_path / "bad.json"
    path.write_text('[\n{"id":"Q1",\n"type":"item"}\n]\n', encoding="utf-8")

    with pytest.raises(ValueError, match=r"bad\.json:2:"):
        list(iter_wikidata_entities(path))
