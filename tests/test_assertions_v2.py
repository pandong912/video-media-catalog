from __future__ import annotations

import pytest

from video_media_catalog.assertions import (
    AssertionProvenance,
    FieldAssertion,
    SourceNodeRef,
    ValueType,
    build_citation,
    build_field_assertion,
    build_relationship_assertion,
)


def _provenance() -> AssertionProvenance:
    return AssertionProvenance(
        envelope_key="sha256:" + ("a" * 64),
        source_path="/name",
        mapper_id="example-mapper",
        mapper_version="1",
        policy_id="example-open",
        policy_digest="sha256:" + ("b" * 64),
        observed_at="2026-09-19T00:00:00Z",
    )


def test_citation_and_field_assertion_are_source_owned() -> None:
    citation = build_citation(
        source_product_id="example-product",
        source_record_id="record-1",
        source_url="https://example.com/records/1",
        retrieved_at="2026-09-19T00:00:00Z",
        raw_object_hash="sha256:" + ("c" * 64),
        attribution_text="Example data.",
    )
    subject = SourceNodeRef(
        namespace_id="example-work",
        source_id="record-1",
        referent_kind="EDITORIAL_WORK",
    )
    assertion = build_field_assertion(
        subject=subject,
        predicate="title",
        value_type=ValueType.STRING,
        value="Example",
        qualifiers={"language": "en"},
        provenance=_provenance().model_copy(
            update={"citation_keys": (citation.citation_key,)}
        ),
    )
    assert assertion.assertion_id.startswith("sha256:")
    assert "entityKey" not in assertion.json_bytes().decode()
    assert citation == type(citation).model_validate_json(citation.json_bytes())


def test_assertion_identity_detects_tampering() -> None:
    assertion = build_field_assertion(
        subject=SourceNodeRef(
            namespace_id="example-work",
            source_id="record-1",
            referent_kind="EDITORIAL_WORK",
        ),
        predicate="title",
        value_type=ValueType.STRING,
        value="Example",
        provenance=_provenance(),
    )
    with pytest.raises(ValueError, match="assertion_id"):
        FieldAssertion.model_validate(
            {
                **assertion.model_dump(mode="python"),
                "value_json": '"Changed"',
            }
        )


def test_field_assertion_rejects_mismatched_value_type() -> None:
    with pytest.raises(ValueError, match="INTEGER"):
        build_field_assertion(
            subject=SourceNodeRef(
                namespace_id="example-work",
                source_id="record-1",
                referent_kind="EDITORIAL_WORK",
            ),
            predicate="runtime_minutes",
            value_type=ValueType.INTEGER,
            value="45",
            provenance=_provenance(),
        )


def test_relationship_assertion_preserves_source_nodes() -> None:
    relation = build_relationship_assertion(
        subject=SourceNodeRef(
            namespace_id="example-episode",
            source_id="episode-1",
            referent_kind="EPISODE",
        ),
        predicate="episode_of",
        object=SourceNodeRef(
            namespace_id="example-series",
            source_id="series-1",
            referent_kind="SERIES",
        ),
        qualifiers={"orderingScheme": "official", "number": "1"},
        provenance=_provenance(),
    )
    assert relation.object.referent_kind == "SERIES"
