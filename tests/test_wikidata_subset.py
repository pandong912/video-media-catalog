from __future__ import annotations

import random

import pytest
from pydantic import ValidationError

from video_media_catalog.models import Checksum, ObjectRef
from video_media_catalog.wikidata_subset import (
    ENTITY_BUDGET_TYPES,
    RelationReference,
    SelectionCandidate,
    SubsetAuditManifest,
    SubsetSelectionConfig,
    classification_dependency_qids,
    prepare_subset_payload,
    prune_relation_statements,
    select_subset_candidates,
    subset_source_manifest_entry,
)


def _candidate(
    qid: str,
    entity_type: str,
    sites: int,
    *relations: tuple[str, str],
) -> SelectionCandidate:
    return SelectionCandidate(
        qid=qid,
        entity_type=entity_type,
        sitelink_count=sites,
        relations=tuple(
            RelationReference(property_id, target) for property_id, target in relations
        ),
    )


def test_budget_selection_is_stable_with_quota_refill_and_reference_priority() -> None:
    candidates = [
        _candidate(
            "Q10",
            "MOVIE",
            5,
            ("P179", "Q30"),
            ("P57", "Q40"),
            ("P161", "Q40"),
        ),
        _candidate("Q11", "MOVIE", 4, ("P161", "Q40")),
        _candidate("Q12", "MOVIE", 3, ("P161", "Q41")),
        _candidate("Q20", "TV_SERIES", 2, ("P272", "Q50")),
        _candidate("Q30", "TV_SERIES", 0),
        _candidate("Q40", "PERSON", 0),
        _candidate("Q41", "PERSON", 0),
        _candidate("Q50", "ORGANIZATION", 0),
    ]
    config = SubsetSelectionConfig(
        target_count=7,
        work_quotas={
            "MOVIE": 1,
            "TV_SERIES": 1,
            "TV_SEASON": 1,
            "TV_EPISODE": 1,
        },
    )

    expected = ("Q10", "Q11", "Q12", "Q20", "Q30", "Q40", "Q41")
    for seed in range(8):
        shuffled = list(candidates)
        random.Random(seed).shuffle(shuffled)
        result = select_subset_candidates(shuffled, config)
        assert result.selected_qids == expected
        assert result.selected_count == config.target_count
        assert result.work_count == 4
        assert result.hierarchy_count == 1
        assert result.credit_count == 2
        assert result.fallback_count == 0


def test_budget_deterministically_falls_back_when_references_are_insufficient() -> None:
    config = SubsetSelectionConfig(
        target_count=3,
        work_quotas={
            "MOVIE": 1,
            "TV_SERIES": 0,
            "TV_SEASON": 0,
            "TV_EPISODE": 0,
        },
    )
    result = select_subset_candidates(
        [
            _candidate("Q5", "PERSON", 1),
            _candidate("Q2", "MOVIE", 2),
            _candidate("Q4", "ORGANIZATION", 1),
        ],
        config,
    )

    assert result.selected_qids == ("Q2", "Q4", "Q5")
    assert result.fallback_count == 2


def _statement(target: str) -> dict:
    return {
        "rank": "normal",
        "mainsnak": {
            "snaktype": "value",
            "datavalue": {
                "type": "wikibase-entityid",
                "value": {"id": target},
            },
        },
    }


def test_dependency_rows_and_relation_pruning_have_no_dangling_targets() -> None:
    payloads = {
        "Q100": {"id": "Q100", "claims": {"P31": [_statement("Q900")]}},
        "Q900": {"id": "Q900", "claims": {"P279": [_statement("Q11424")]}},
        "Q11424": {"id": "Q11424", "claims": {}},
    }
    assert classification_dependency_qids(["Q100"], payloads) == (
        "Q900",
        "Q11424",
    )

    payload = {
        "id": "Q100",
        "claims": {
            "P57": [_statement("Q200"), _statement("Q201")],
            "P179": [_statement("Q300")],
            "P345": [{"rank": "normal", "mainsnak": {"snaktype": "novalue"}}],
            "P577": [{"rank": "normal", "mainsnak": {"snaktype": "novalue"}}],
        },
    }
    pruned, count = prune_relation_statements(payload, {"Q100", "Q200"})

    assert count == 2
    assert len(pruned["claims"]["P57"]) == 1
    assert "P179" not in pruned["claims"]
    assert "P345" in pruned["claims"]
    assert "P577" in pruned["claims"]

    dependency, _ = prepare_subset_payload(
        payloads["Q100"],
        {"Q100"},
        dependency_row=True,
    )
    assert "P31" not in dependency["claims"]


def _ref(kind: str) -> ObjectRef:
    return ObjectRef(
        uri=f"s3://catalog/{kind}",
        format=(
            "OBJECT_FORMAT_PARQUET"
            if kind == "manifest.parquet"
            else "OBJECT_FORMAT_JSON"
        ),
        media_type=(
            "application/vnd.apache.parquet"
            if kind == "manifest.parquet"
            else "application/x-bzip2"
        ),
        checksum=Checksum(value="a" * 64),
        size_bytes=123,
        etag="etag-1",
        object_version="version-1",
    )


def test_audit_manifest_strictly_binds_counts_config_and_object_refs() -> None:
    config = SubsetSelectionConfig(
        target_count=2,
        work_quotas={
            "MOVIE": 1,
            "TV_SERIES": 0,
            "TV_SEASON": 0,
            "TV_EPISODE": 0,
        },
    )
    counts = {entity_type: 0 for entity_type in ENTITY_BUDGET_TYPES}
    counts["MOVIE"] = 1
    counts["PERSON"] = 1
    audit = SubsetAuditManifest(
        config_digest=config.digest,
        dump=_ref("dump.json.bz2"),
        subset=_ref("subset.json.bz2"),
        source_manifest=_ref("manifest.parquet"),
        target_count=config.target_count,
        work_quotas=config.work_quotas,
        selected_count=2,
        selected_counts=counts,
        dependency_rows=3,
        pruned_relation_statements=4,
        normalization_staging_uri="s3://catalog/staging",
    )

    payload = audit.model_dump(mode="json", by_alias=True)
    assert payload["selectedCount"] == "2"
    assert payload["dump"]["objectVersion"] == "version-1"
    assert payload["sourceManifest"]["etag"] == "etag-1"

    with pytest.raises(ValidationError, match="Extra inputs"):
        SubsetAuditManifest.model_validate({**audit.model_dump(), "unexpected": True})


def test_runtime_source_manifest_entry_has_complete_immutable_fields() -> None:
    subset = _ref("subset.json.bz2")
    entry = subset_source_manifest_entry(subset)

    assert entry.source == "wikidata"
    assert entry.uri == subset.uri
    assert entry.sha256 == subset.checksum.value
    assert entry.size_bytes == subset.size_bytes
    assert entry.compression == "bzip2"
    assert entry.object_version == "version-1"
    assert entry.etag == "etag-1"
    assert entry.license == "CC0-1.0"


def test_normalized_dump_row_skips_property_entities() -> None:
    import json

    from video_media_catalog.wikidata_subset_spark import _normalized_dump_row

    property_line = json.dumps(
        {
            "id": "P10027",
            "type": "property",
            "labels": {},
            "descriptions": {},
            "aliases": {},
            "sitelinks": {},
            "claims": {},
        }
    )
    item_line = json.dumps(
        {
            "id": "Q42",
            "type": "item",
            "labels": {},
            "descriptions": {},
            "aliases": {},
            "sitelinks": {},
            "claims": {},
        }
    )
    assert _normalized_dump_row(property_line) is None
    row = _normalized_dump_row(item_line)
    assert row is not None
    assert row["qid"] == "Q42"
    assert row["qid_numeric"] == 42
