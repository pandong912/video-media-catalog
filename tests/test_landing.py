from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from video_media_catalog.landing import extract_landing
from video_media_catalog.models import LandingManifest, LandingSummary
from video_media_catalog.storage import digest_file, local_path


def test_writes_deterministic_shards_manifest_and_summary_last(
    fixture_dir: Path, tmp_path: Path
) -> None:
    output = tmp_path / "landing-output"
    first = extract_landing(
        output_uri=output.as_uri(),
        wikidata_uri=str(fixture_dir / "wikidata.json"),
        eidr_xml_uri=(fixture_dir / "eidr.xml").as_uri(),
        shard_records=4,
    )
    manifest_path = output / "landing-manifest.json"
    summary_path = output / "landing-summary.json"
    manifest_bytes = manifest_path.read_bytes()
    summary_bytes = summary_path.read_bytes()
    manifest = LandingManifest.model_validate_json(manifest_bytes)
    summary = LandingSummary.model_validate_json(summary_bytes)
    shard_digests = [digest_file(local_path(shard.uri))[0] for shard in manifest.shards]

    second = extract_landing(
        output_uri=str(output),
        wikidata_uri=str(fixture_dir / "wikidata.json"),
        eidr_xml_uri=str(fixture_dir / "eidr.xml"),
        shard_records=4,
    )

    assert first == second
    assert manifest_path.read_bytes() == manifest_bytes
    assert summary_path.read_bytes() == summary_bytes
    assert [digest_file(local_path(shard.uri))[0] for shard in manifest.shards] == (
        shard_digests
    )
    assert manifest.record_count == 20
    assert manifest.source_counts == {"wikidata": 14, "eidr": 6}
    assert len(manifest.shards) == 5
    assert summary.status == "COMPLETE"
    assert summary.manifest_checksum == digest_file(manifest_path)[0]
    assert (
        sum(pq.read_table(local_path(shard.uri)).num_rows for shard in manifest.shards)
        == 20
    )


def test_does_not_publish_completion_marker_after_parse_failure(
    fixture_dir: Path, tmp_path: Path
) -> None:
    bad_xml = tmp_path / "bad.xml"
    bad_xml.write_text("<broken>", encoding="utf-8")
    output = tmp_path / "failed"

    with pytest.raises(ValueError):
        extract_landing(
            output_uri=str(output),
            wikidata_uri=str(fixture_dir / "wikidata.json"),
            eidr_xml_uri=str(bad_xml),
            shard_records=20,
        )

    assert not (output / "landing-summary.json").exists()
    assert not (output / "landing-manifest.json").exists()


def test_rejects_expected_source_checksum_mismatch(
    fixture_dir: Path, tmp_path: Path
) -> None:
    with pytest.raises(ValueError, match="checksum mismatch"):
        extract_landing(
            output_uri=str(tmp_path / "output"),
            wikidata_uri=str(fixture_dir / "wikidata.json"),
            wikidata_sha256="0" * 64,
        )
