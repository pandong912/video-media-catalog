from __future__ import annotations

from pathlib import Path

import pytest

from video_media_catalog.eidr import (
    EidrProviderNotConfiguredError,
    fetch_exact,
    iter_eidr_payloads,
    iter_eidr_records,
    normalize_eidr_id,
    normalize_imdb_id,
)


def test_namespace_tolerant_offline_parser_extracts_metadata(
    fixture_dir: Path,
) -> None:
    records = list(iter_eidr_payloads(fixture_dir / "eidr.xml"))
    assert len(records) == 6

    movie = records[0]
    assert movie["id"] == "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"
    assert movie["referentType"] == "Movie"
    assert movie["releaseDate"] == "2020-01-02"
    assert movie["duration"] == "PT2H"
    assert movie["countries"] == ["US"]
    assert {title["language"] for title in movie["titles"]} == {"en", "zh"}
    assert {"scheme": "imdb", "value": "tt0000001"} in movie["alternateIds"]

    relation_types = {
        relation["type"] for record in records for relation in record["parentRelations"]
    }
    assert relation_types == {
        "PART_OF_SERIES",
        "PART_OF_SEASON",
        "EDIT_OF",
    }
    assert records[2]["recordType"] == "SERIES"
    assert records[3]["recordType"] == "SEASON"
    assert records[4]["recordType"] == "EPISODE"
    assert records[4]["referentType"] == "TV"
    assert records[5]["recordType"] == "EDIT"


def test_eidr_landing_records_are_stable(fixture_dir: Path) -> None:
    first = list(iter_eidr_records(fixture_dir / "eidr.xml"))
    second = list(iter_eidr_records(fixture_dir / "eidr.xml"))
    assert first == second
    assert first[0].source_revision == "2026-03-01T00:00:00Z"


def test_network_lookup_requires_explicit_provider() -> None:
    with pytest.raises(EidrProviderNotConfiguredError):
        fetch_exact(eidr_ids=["10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"])


def test_rejects_dtd_and_entities(tmp_path: Path) -> None:
    path = tmp_path / "unsafe.xml"
    path.write_text(
        '<!DOCTYPE x [<!ENTITY value "unsafe">]>'
        "<FullMetadata><ID>&value;</ID></FullMetadata>",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="DOCTYPE"):
        list(iter_eidr_payloads(path))


def test_exact_provider_never_receives_titles() -> None:
    calls: list[tuple[str, str]] = []

    class Provider:
        def fetch_by_eidr_id(self, eidr_id: str) -> bytes:
            calls.append(("eidr", eidr_id))
            return b"<record/>"

        def fetch_by_imdb_id(self, imdb_id: str) -> bytes:
            calls.append(("imdb", imdb_id))
            return b"<record/>"

    assert (
        len(
            list(
                fetch_exact(
                    eidr_ids=["10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"],
                    imdb_ids=["TT0000001"],
                    provider=Provider(),
                )
            )
        )
        == 2
    )
    assert calls == [
        ("eidr", "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"),
        ("imdb", "tt0000001"),
    ]


def test_identifier_normalization_enforces_public_formats() -> None:
    assert normalize_imdb_id("NM0000001") == "nm0000001"
    assert (
        normalize_eidr_id("https://doi.org/10.5240/aaaa-bbbb-cccc-dddd-eeee-c")
        == "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"
    )
    with pytest.raises(ValueError, match="invalid EIDR"):
        normalize_eidr_id("10.5240/TOO-SHORT")
