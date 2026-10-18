from __future__ import annotations

import copy
import hashlib
import json
from collections import Counter
from pathlib import Path

import pytest
from pydantic import ValidationError

from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_sources import build_community_registry
from video_media_catalog.connector import build_connector_record_set_manifest
from video_media_catalog.gold_search_projection import project_gold_entity
from video_media_catalog.music_pilot import (
    MusicPilotCoordinateDisposition,
    MusicPilotMatchProfile,
    build_music_coordinate_review_conflict,
    build_music_pilot_match_profile,
    load_music_pilot_cohort,
    load_music_pilot_match_profile,
    match_music_pilot_asset,
    replay_music_pilot,
)
from video_media_catalog.reference_fixture import (
    MaterializedReferenceFixture,
    load_reference_fixture,
    materialize_reference_fixture,
)
from video_media_catalog.source_silver import build_source_silver_rows

FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "research"
ALGORITHM_PATH = (
    Path(__file__).parent / "fixtures" / ("catalog-gold-match-algorithm.v1.json")
)
PROFILE_PATH = FIXTURE_ROOT / "music-pilot-match-profile.v1.json"
CAPTURE_TOOL = (
    Path(__file__).parents[1] / "tools" / ("capture_music_pilot_reference.py")
)
TIMESTAMP = "2026-10-17T03:00:00Z"

EXACT_COORDINATES = {
    ("Q66662496", "9aa56ede-83be-44ea-aeb5-d61268972c53"),
    ("Q105942412", "321d235c-a32a-4f6c-b84d-0e151ffb58be"),
    ("Q105978624", "e1ded706-16ec-45c3-87c4-0ae7a26f56d3"),
    ("Q106098098", "b611eecb-2e93-46ad-aaf4-f00f02a913ff"),
    ("Q106182588", "020152a7-172e-45c5-812a-a60d02cb2a27"),
    ("Q106435189", "61b03fdf-24aa-4c82-b5d6-1e25691e8347"),
    ("Q109795255", "c40d7b44-7583-4675-852a-10ddbea8cf46"),
    ("Q113148225", "7fb6969c-bb00-48ec-84df-151fd56875c2"),
}
LIVE_WORK_COORDINATES = {
    ("Q2607570", "9dc29ea6-7a6f-3cc5-aa37-560b04afb0d7"),
    ("Q17591232", "4f5a74d0-d183-37f1-b07a-913652e82667"),
    ("Q138836655", "8b3a9214-bbec-311d-a61b-874bf15dfdde"),
    ("Q908534", "a488a020-884d-3349-a7e7-18993ffed13e"),
    ("Q960586", "e9914523-2452-3987-8d4d-08f8e4f3c302"),
    ("Q1140459", "660d0d25-4253-4d30-a818-61fc94e375bf"),
    ("Q12860108", "ba28e7e2-862e-3438-9faa-13ebb6e9030c"),
    ("Q29117946", "8d331505-4d88-39ae-81c7-bec5da77af96"),
}
CONFLICT_COORDINATES = {
    ("Q113151843", "d9da08d3-3c24-4f4c-b1e7-beeee60bab08"),
    ("Q113153791", "71d7fa30-ae9e-431b-9fce-a6ecb63f3cfa"),
}
VARIANT_COORDINATE = (
    "Q108042886",
    "b70b3468-8f1a-4fa3-b853-122f57d98f92",
)
MANUAL_WORK_QIDS = {"Q138836655", "Q960586", "Q12860108"}


def _materialize(name: str) -> MaterializedReferenceFixture:
    return materialize_reference_fixture(load_reference_fixture(FIXTURE_ROOT / name))


def _algorithm_contract() -> dict[str, object]:
    return json.loads(ALGORITHM_PATH.read_text(encoding="utf-8"))


def _pilot_material() -> tuple[
    object,
    MaterializedReferenceFixture,
    MaterializedReferenceFixture,
    MusicPilotMatchProfile,
]:
    cohort = load_music_pilot_cohort(FIXTURE_ROOT / "music-pilot-cohort.v1.json")
    musicbrainz = _materialize("musicbrainz-music-pilot.v1.json")
    wikidata = _materialize("wikidata-music-pilot.v1.json")
    profile = build_music_pilot_match_profile(
        cohort=cohort,
        musicbrainz=musicbrainz,
        wikidata=wikidata,
        algorithm_contract=_algorithm_contract(),
    )
    return cohort, musicbrainz, wikidata, profile


def _silver_rows(fixture: MaterializedReferenceFixture):
    captured = fixture.captured
    record_set = build_connector_record_set_manifest(
        batch_id=captured.batch_manifest.batch_id,
        source_product_id=captured.batch_manifest.source_product_id,
        policy_id=captured.batch_manifest.policy_id,
        policy_digest=captured.batch_manifest.policy_digest,
        record_objects=captured.pin.raw_objects,
        record_count=len(captured.envelopes),
        first_envelope_key=captured.envelopes[0].envelope_key,
        last_envelope_key=captured.envelopes[-1].envelope_key,
        created_at=captured.pin.captured_at,
    )
    return build_source_silver_rows(
        registry=build_community_registry(),
        batch=captured.batch_manifest,
        record_set=record_set,
        envelopes=captured.envelopes,
    )


def test_real_authority_capture_is_pinned_complete_and_replayable() -> None:
    cohort, musicbrainz, wikidata, _profile = _pilot_material()

    assert cohort == load_music_pilot_cohort(
        FIXTURE_ROOT / "music-pilot-cohort.v1.json"
    )
    assert musicbrainz == _materialize("musicbrainz-music-pilot.v1.json")
    assert wikidata == _materialize("wikidata-music-pilot.v1.json")
    assert cohort.cohort_digest == (
        "sha256:ff44f41739c8967fb5d12471961489263566db9c27534177b0b3f1bc6b67884c"
    )
    assert musicbrainz.fixture.source_revision == (
        "sha256:53af4e3cece1f12d26accd1cfa080e31932a637a3f31435723badc5cf8b3a46f"
    )
    assert wikidata.fixture.source_revision == (
        "sha256:48949106cf45ed8ebdc26bdd41060054e5d3b492e934a85b9e83c13f6fad067f"
    )
    assert {
        cohort.config_digest,
        musicbrainz.fixture.config_digest,
        wikidata.fixture.config_digest,
    } == {"sha256:c5b99383a27231eb94d63b27b74ee2bf45558c825e46d12837200d841180dfba"}
    assert musicbrainz.fixture.digest == (
        "sha256:64ba951e8d2991f3b5923572d79a5d79e917ebbc2b690e6630222a820fa3db7b"
    )
    assert wikidata.fixture.digest == (
        "sha256:e3d56e2e2ca6dd0b715b9dca802e4288b80714f3b8462d4cbdd2b7022b7b726b"
    )
    assert musicbrainz.captured.pin.pin_id == (
        "sha256:2df1c74f4d811dbdcaeca44a5c3166115569e10da83e8a79c916963097c79dd9"
    )
    assert wikidata.captured.pin.pin_id == (
        "sha256:c34666ea9c95931dd8afd36ca293f59c4aa548c294868ed3547d3a55a6f44d65"
    )
    capture_tool_digest = (
        "sha256:" + hashlib.sha256(CAPTURE_TOOL.read_bytes()).hexdigest()
    )
    assert {
        musicbrainz.fixture.image_digest,
        wikidata.fixture.image_digest,
    } == {capture_tool_digest}
    assert len(musicbrainz.mapped) == 42
    assert len(wikidata.mapped) == 41
    assert len(musicbrainz.fixture.capture_receipts) == 27
    assert len(wikidata.fixture.capture_receipts) == 2

    all_coordinates = {
        (item.wikidata_qid, item.musicbrainz_mbid)
        for item in cohort.authority_coordinates
    }
    assert all_coordinates == (
        EXACT_COORDINATES
        | LIVE_WORK_COORDINATES
        | CONFLICT_COORDINATES
        | {VARIANT_COORDINATE}
    )
    assert {
        item.wikidata_qid
        for item in cohort.authority_coordinates
        if item.coordinate_review.mapping_kind.startswith("RESEARCH_CANDIDATE_")
    } == MANUAL_WORK_QIDS
    conflict_assets = [
        item for item in cohort.assets if item.case == "CONFLICTING_RECORDINGS"
    ]
    assert len(conflict_assets) == 1
    conflict = conflict_assets[0]
    assert {
        (
            item.wikidata_qid,
            item.musicbrainz_mbid,
        )
        for item in conflict.features.authority_coordinates
    } == CONFLICT_COORDINATES
    assert conflict.features.title == "12-the-doctor.flac"
    assert conflict.features.conflict_evidence is not None
    assert (
        conflict.features.conflict_evidence.wikidata_shared_media_object
        == conflict.features.title
    )
    variant = next(
        item for item in cohort.assets if item.case == "REMIX_OR_MEDIA_VARIANT"
    )
    assert variant.features.title == (
        "Там, за чертой (DIP Project remix full version).mp3"
    )
    assert variant.features.variant_evidence is not None
    assert (
        variant.features.variant_evidence.musicbrainz_recording_relation.target_id
        == "ae81a568-f6f4-4a69-9da0-b8561247642d"
    )
    no_authority_titles = {
        item.features.title
        for item in cohort.assets
        if item.case == "NO_AUTHORITY_COORDINATE"
    }
    assert no_authority_titles == {
        "Annette Hanshaw Big City Blues.ogg",
        (
            "1925 (USA) Archives 1925 03 19 Ben Bernie Orchestra - "
            "Sweet Georgia Brown.mp3"
        ),
    }
    authority_titles = {
        json.loads(assertion.value_json)
        for fixture in (musicbrainz, wikidata)
        for mapped in fixture.mapped
        for assertion in mapped.field_assertions
        if assertion.predicate == "title"
    }
    assert no_authority_titles.isdisjoint(authority_titles)

    wikidata_by_id = {item.source_node.source_id: item for item in wikidata.mapped}
    musicbrainz_ids = {item.source_node.source_id for item in musicbrainz.mapped}
    for coordinate in cohort.authority_coordinates:
        assert coordinate.musicbrainz_mbid in musicbrainz_ids
        identifiers = {
            (item.namespace_id, item.value)
            for item in wikidata_by_id[coordinate.wikidata_qid].identifier_assertions
        }
        assert ("wikidata-item", coordinate.wikidata_qid) in identifiers
        expected_namespace = (
            "musicbrainz-recording"
            if coordinate.entity_type == "MUSIC_RECORDING"
            else "musicbrainz-work"
        )
        if coordinate.wikidata_qid in MANUAL_WORK_QIDS:
            assert (expected_namespace, coordinate.musicbrainz_mbid) not in identifiers
            assert coordinate.coordinate_review.review_status == "PENDING"
        else:
            assert (expected_namespace, coordinate.musicbrainz_mbid) in identifiers
            assert coordinate.coordinate_review.mapping_kind == "AUTHORITY_DECLARED"

    for fixture in (musicbrainz, wikidata):
        projected_ids = {
            item
            for receipt in fixture.fixture.capture_receipts
            for item in receipt.projected_record_ids
        }
        assert {item.source_node.source_id for item in fixture.mapped} <= projected_ids
        assert all(
            receipt.request_url.startswith("https://")
            and receipt.response_hash.startswith("sha256:")
            and receipt.authority_revision
            and receipt.projection_paths
            for receipt in fixture.fixture.capture_receipts
        )
        assert all(
            assertion.provenance.citation_keys
            for mapped in fixture.mapped
            for assertion in (
                *mapped.field_assertions,
                *mapped.identifier_assertions,
                *mapped.relationship_assertions,
                *mapped.entity_type_assertions,
            )
        )

    tampered = musicbrainz.fixture.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )
    tampered["captureReceipts"][0]["responseHash"] = "sha256:" + ("0" * 64)
    with pytest.raises(ValidationError, match="source revision"):
        type(musicbrainz.fixture).model_validate(tampered)


def test_pilot_authority_records_flow_through_existing_silver_tables() -> None:
    _cohort, musicbrainz, wikidata, _profile = _pilot_material()
    musicbrainz_run, musicbrainz_rows = _silver_rows(musicbrainz)
    wikidata_run, wikidata_rows = _silver_rows(wikidata)

    assert musicbrainz_run.input_manifest["sourceSnapshotPinId"] == (
        musicbrainz.captured.pin.pin_id
    )
    assert wikidata_run.input_manifest["sourceSnapshotPinId"] == (
        wikidata.captured.pin.pin_id
    )
    assert {table: len(rows) for table, rows in musicbrainz_rows.items() if rows} == {
        "community_source_record": 42,
        "community_field_assertion": 152,
        "community_identifier_assertion": 56,
        "community_relationship_assertion": 49,
        "community_entity_type_assertion": 42,
    }
    assert {table: len(rows) for table, rows in wikidata_rows.items() if rows} == {
        "community_source_record": 41,
        "community_field_assertion": 264,
        "community_identifier_assertion": 83,
        "community_relationship_assertion": 55,
        "community_entity_type_assertion": 41,
    }
    assert {
        row["predicate"] for row in musicbrainz_rows["community_field_assertion"]
    } == {"media_variant", "music_agent_kind", "title"}
    assert {row["predicate"] for row in wikidata_rows["community_field_assertion"]} == {
        "media_object",
        "music_agent_kind",
        "title",
    }
    for rows in (musicbrainz_rows, wikidata_rows):
        assert {
            row["predicate"] for row in rows["community_relationship_assertion"]
        } == {"credit", "recording_of"}
        assert {
            row["entity_type"] for row in rows["community_entity_type_assertion"]
        } == {"MUSIC_ARTIST", "MUSIC_RECORDING", "MUSICAL_WORK"}
        assert all(
            json.loads(row["provenance_json"])["citationKeys"]
            for table, table_rows in rows.items()
            if table.endswith("_assertion")
            for row in table_rows
        )
    wikidata_by_id = {item.source_node.source_id: item for item in wikidata.mapped}

    def agent_kind(qid: str) -> str:
        assertion = next(
            item
            for item in wikidata_by_id[qid].field_assertions
            if item.predicate == "music_agent_kind"
        )
        return json.loads(assertion.value_json)

    assert agent_kind("Q1268") == "PERSON"
    assert agent_kind("Q100269171") == "ORGANIZATION"
    assert {
        assertion.qualifiers.get("agentKind")
        for mapped in wikidata.mapped
        for assertion in mapped.relationship_assertions
        if assertion.object.referent_kind == "MUSIC_ARTIST"
    } == {"PERSON", "ORGANIZATION"}
    for fixture in (musicbrainz, wikidata):
        relation_facts = [
            (
                mapped.source_node.namespace_id,
                mapped.source_node.source_id,
                assertion.predicate,
                assertion.object.namespace_id,
                assertion.object.source_id,
                canonical_json(assertion.qualifiers),
            )
            for mapped in fixture.mapped
            for assertion in mapped.relationship_assertions
        ]
        assert len(relation_facts) == len(set(relation_facts))


def test_concrete_profile_and_twenty_asset_match_baseline_are_deterministic() -> None:
    cohort, musicbrainz, wikidata, profile = _pilot_material()
    replayed_profile = build_music_pilot_match_profile(
        cohort=cohort,
        musicbrainz=musicbrainz,
        wikidata=wikidata,
        algorithm_contract=_algorithm_contract(),
    )
    pinned_profile = load_music_pilot_match_profile(PROFILE_PATH)

    assert profile == replayed_profile == pinned_profile
    assert profile.profile_id == (
        "sha256:b9b5e83d8676f5bd99d923c4f052ea74500e2df3fd00f7336f6f62b62fdb5a61"
    )
    assert profile.authority_coordinate_digest == (
        "sha256:f7acf2518c54acb103b044de38cc72b67cb02a593adfcf31c12a88b17ebe053a"
    )
    assert profile.algorithm_digest == (
        "sha256:638d10b15510a0d81e224692fdd5d1b9e35112bed1de0b9f289a56f9c233116d"
    )
    assert profile.production_activation_allowed is False
    assert profile.cutover_allowed is False
    assert profile.validated_index_manifest_required is True
    assert profile.concrete_index.startswith("media-catalog-research-")
    assert profile.coordinate_count == 19
    assert profile.source_fixture_digests == {
        "musicbrainz-core-dump": musicbrainz.fixture.digest,
        "wikidata-music-reference": wikidata.fixture.digest,
    }
    assert set(profile.capture_image_digests.values()) == {
        musicbrainz.fixture.image_digest
    }
    assert Counter(item.disposition for item in profile.coordinates) == {
        MusicPilotCoordinateDisposition.EXACT_RECORDING: 8,
        MusicPilotCoordinateDisposition.WORK_PARENT_ONLY: 8,
        MusicPilotCoordinateDisposition.CONFLICT_ONLY: 2,
        MusicPilotCoordinateDisposition.VARIANT_ABSTAIN_ONLY: 1,
    }

    first = replay_music_pilot(cohort, profile=profile)
    second = replay_music_pilot(cohort, profile=profile)
    assert first == second
    assert Counter(item.decision.value for item in first) == {
        "EXACT": 8,
        "REVIEW_REQUIRED": 8,
        "ABSTAIN": 4,
    }
    assert Counter(item.case for item in cohort.assets) == {
        "EXACT_RECORDING": 8,
        "LIVE_WORK_PARENT": 8,
        "CONFLICTING_RECORDINGS": 1,
        "REMIX_OR_MEDIA_VARIANT": 1,
        "NO_AUTHORITY_COORDINATE": 2,
    }
    for asset, result in zip(cohort.assets, first, strict=True):
        assert result.decision == asset.expected.decision
        assert result.reason == asset.expected.reason
        if asset.expected.candidate_entity_type is not None:
            assert result.candidate_entity_type == asset.expected.candidate_entity_type
        if asset.expected.candidate_namespace is not None:
            assert result.candidate_namespace == asset.expected.candidate_namespace
        if asset.expected.candidate_id is not None:
            assert result.candidate_id == asset.expected.candidate_id
        if asset.case == "LIVE_WORK_PARENT":
            assert result.decision.value == "REVIEW_REQUIRED"
            assert result.candidate_entity_type == "MUSICAL_WORK"
            assert result.candidate_namespace == "musicbrainz-work"
        if asset.case == "REMIX_OR_MEDIA_VARIANT":
            assert result.decision.value == "ABSTAIN"
            assert result.candidate_id is None

    match_args = {
        "profile": profile,
        "source_policy_digest": profile.source_policy_digest,
        "authority_coordinate_digest": profile.authority_coordinate_digest,
        "algorithm_digest": profile.algorithm_digest,
    }
    variant = next(
        item for item in cohort.assets if item.case == "REMIX_OR_MEDIA_VARIANT"
    )
    disguised_variant = variant.model_copy(
        update={
            "features": variant.features.model_copy(
                update={
                    "performance_type": "RECORDED",
                    "variant_evidence": None,
                }
            )
        }
    )
    assert (
        match_music_pilot_asset(disguised_variant, **match_args).decision.value
        == "ABSTAIN"
    )
    conflict = next(
        item for item in cohort.assets if item.case == "CONFLICTING_RECORDINGS"
    )
    isolated_conflict = conflict.model_copy(
        update={
            "features": conflict.features.model_copy(
                update={
                    "authority_coordinates": (
                        conflict.features.authority_coordinates[0],
                    )
                }
            )
        }
    )
    assert (
        match_music_pilot_asset(isolated_conflict, **match_args).decision.value
        == "ABSTAIN"
    )
    live = next(item for item in cohort.assets if item.case == "LIVE_WORK_PARENT")
    disguised_live = live.model_copy(
        update={
            "features": live.features.model_copy(
                update={"performance_type": "RECORDED"}
            )
        }
    )
    assert (
        match_music_pilot_asset(disguised_live, **match_args).decision.value
        == "ABSTAIN"
    )


def test_match_and_profile_reject_policy_authority_and_algorithm_drift() -> None:
    cohort, musicbrainz, wikidata, profile = _pilot_material()
    asset = cohort.assets[0]
    valid = {
        "profile": profile,
        "source_policy_digest": profile.source_policy_digest,
        "authority_coordinate_digest": profile.authority_coordinate_digest,
        "algorithm_digest": profile.algorithm_digest,
    }
    for key in (
        "source_policy_digest",
        "authority_coordinate_digest",
        "algorithm_digest",
    ):
        values = {**valid, key: "sha256:" + ("0" * 64)}
        with pytest.raises(ValueError, match="differs from profile"):
            match_music_pilot_asset(asset, **values)

    stale_contract = copy.deepcopy(_algorithm_contract())
    stale_contract["musicMatchPolicy"]["authorityDigestRequired"] = False
    with pytest.raises(ValueError, match="fail-closed abstention"):
        build_music_pilot_match_profile(
            cohort=cohort,
            musicbrainz=musicbrainz,
            wikidata=wikidata,
            algorithm_contract=stale_contract,
        )

    stale_profile = profile.model_dump(
        mode="json",
        by_alias=True,
        exclude_none=True,
    )
    stale_profile["sourcePolicyDigest"] = "sha256:" + ("0" * 64)
    with pytest.raises(ValidationError, match="source policy is not canonical"):
        MusicPilotMatchProfile.model_validate(stale_profile)


def test_unreviewed_cross_authority_work_coordinates_stage_identity_conflicts() -> None:
    cohort, _musicbrainz, _wikidata, profile = _pilot_material()
    candidates = [
        item
        for item in cohort.authority_coordinates
        if item.coordinate_review.mapping_kind == "RESEARCH_CANDIDATE_SAME_WORK_PARENT"
    ]
    assert {item.wikidata_qid for item in candidates} == MANUAL_WORK_QIDS

    conflicts = [
        build_music_coordinate_review_conflict(
            coordinate,
            candidate_entity_keys=(
                "sha256:" + ("1" * 64),
                "sha256:" + ("2" * 64),
            ),
            assertion_keys=("sha256:" + ("3" * 64),),
            observed_at=TIMESTAMP,
            authority_coordinate_digest=profile.authority_coordinate_digest,
        )
        for coordinate in candidates
    ]
    assert len({item.conflict_key for item in conflicts}) == 3
    assert all(
        item.reason == "RESEARCH_CANDIDATE_MUSIC_AUTHORITY_COORDINATE"
        and item.details["reviewStatus"] == "PENDING"
        and item.source_node.namespace_id == "music-coordinate-review"
        for item in conflicts
    )
    authority_declared = next(
        item
        for item in cohort.authority_coordinates
        if item.coordinate_review.mapping_kind == "AUTHORITY_DECLARED"
    )
    with pytest.raises(ValueError, match="does not require curation"):
        build_music_coordinate_review_conflict(
            authority_declared,
            candidate_entity_keys=(
                "sha256:" + ("1" * 64),
                "sha256:" + ("2" * 64),
            ),
            assertion_keys=("sha256:" + ("3" * 64),),
            observed_at=TIMESTAMP,
            authority_coordinate_digest=profile.authority_coordinate_digest,
        )


def _entity_key(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def _trace(
    assertion_id: str,
    *,
    source_product_id: str,
    source_record_id: str,
    source_path: str,
) -> str:
    wikidata = source_product_id == "wikidata-music-reference"
    return canonical_json(
        {
            "assertions": [
                {
                    "assertionId": assertion_id,
                    "sourceProductId": source_product_id,
                    "sourceName": (
                        "Wikidata JSON entity dump"
                        if wikidata
                        else "MusicBrainz core database dump"
                    ),
                    "sourceRecordId": source_record_id,
                    "sourcePath": source_path,
                    "observedAt": TIMESTAMP,
                    "citationKeys": ["sha256:" + ("9" * 64)],
                    "rights": {
                        "policyId": (
                            "wikidata-structured-data-cc0"
                            if wikidata
                            else "musicbrainz-core-data-cc0"
                        ),
                        "policyZone": "open_cc0",
                        "licenseId": "CC0-1.0",
                        "licenseUri": (
                            "https://creativecommons.org/publicdomain/zero/1.0/"
                        ),
                        "attributionText": (
                            "Wikidata structured data (CC0)."
                            if wikidata
                            else "MusicBrainz core database contributors"
                        ),
                        "sourceUrl": (
                            "https://www.wikidata.org/"
                            if wikidata
                            else "https://musicbrainz.org/"
                        ),
                        "shareAlike": False,
                    },
                }
            ]
        }
    )


def _project_recording_coordinate(
    *,
    qid: str,
    mbid: str,
    musicbrainz: MaterializedReferenceFixture,
    wikidata: MaterializedReferenceFixture,
) -> dict[str, object]:
    musicbrainz_record = next(
        item for item in musicbrainz.mapped if item.source_node.source_id == mbid
    )
    wikidata_record = next(
        item for item in wikidata.mapped if item.source_node.source_id == qid
    )
    title = next(
        item
        for item in musicbrainz_record.field_assertions
        if item.predicate == "title"
    )
    fields = [
        {
            "predicate": "title",
            "value_json": title.value_json,
            "qualifiers_json": canonical_json(title.qualifiers),
            "resolution_status": "SELECTED",
            "selected_assertion_id": title.assertion_id,
            "assertion_ids_json": canonical_json([title.assertion_id]),
            "trace_json": _trace(
                title.assertion_id,
                source_product_id="musicbrainz-core-dump",
                source_record_id=mbid,
                source_path=title.provenance.source_path,
            ),
        }
    ]
    for source_product_id, source_record_id, mapped in (
        ("musicbrainz-core-dump", mbid, musicbrainz_record),
        ("wikidata-music-reference", qid, wikidata_record),
    ):
        for assertion in mapped.field_assertions:
            if (
                assertion.predicate == "title"
                and assertion.assertion_id != title.assertion_id
            ):
                fields.append(
                    {
                        "predicate": assertion.predicate,
                        "value_json": assertion.value_json,
                        "qualifiers_json": canonical_json(assertion.qualifiers),
                        "resolution_status": "SET",
                        "selected_assertion_id": None,
                        "assertion_ids_json": canonical_json([assertion.assertion_id]),
                        "trace_json": _trace(
                            assertion.assertion_id,
                            source_product_id=source_product_id,
                            source_record_id=source_record_id,
                            source_path=assertion.provenance.source_path,
                        ),
                    }
                )
            if assertion.predicate not in {"media_object", "media_variant"}:
                continue
            fields.append(
                {
                    "predicate": assertion.predicate,
                    "value_json": assertion.value_json,
                    "qualifiers_json": canonical_json(assertion.qualifiers),
                    "resolution_status": "SET",
                    "selected_assertion_id": None,
                    "assertion_ids_json": canonical_json([assertion.assertion_id]),
                    "trace_json": _trace(
                        assertion.assertion_id,
                        source_product_id=source_product_id,
                        source_record_id=source_record_id,
                        source_path=assertion.provenance.source_path,
                    ),
                }
            )
    recording_identifier = next(
        item
        for item in musicbrainz_record.identifier_assertions
        if item.namespace_id == "musicbrainz-recording"
    )
    wikidata_identifier = next(
        item
        for item in wikidata_record.identifier_assertions
        if item.namespace_id == "wikidata-item"
    )
    identifiers = [
        {
            "namespace_id": recording_identifier.namespace_id,
            "value": recording_identifier.value,
            "issuer": recording_identifier.issuer,
            "referent_kind": recording_identifier.referent_kind,
            "assertion_ids_json": canonical_json([recording_identifier.assertion_id]),
            "trace_json": _trace(
                recording_identifier.assertion_id,
                source_product_id="musicbrainz-core-dump",
                source_record_id=mbid,
                source_path=recording_identifier.provenance.source_path,
            ),
        },
        {
            "namespace_id": wikidata_identifier.namespace_id,
            "value": wikidata_identifier.value,
            "issuer": wikidata_identifier.issuer,
            "referent_kind": wikidata_identifier.referent_kind,
            "assertion_ids_json": canonical_json([wikidata_identifier.assertion_id]),
            "trace_json": _trace(
                wikidata_identifier.assertion_id,
                source_product_id="wikidata-music-reference",
                source_record_id=qid,
                source_path=wikidata_identifier.provenance.source_path,
            ),
        },
    ]
    relations = []
    for source_product_id, source_record_id, mapped in (
        ("musicbrainz-core-dump", mbid, musicbrainz_record),
        ("wikidata-music-reference", qid, wikidata_record),
    ):
        for assertion in mapped.relationship_assertions:
            if assertion.predicate not in {"recording_of", "credit"}:
                continue
            relations.append(
                {
                    "predicate": assertion.predicate,
                    "object_entity_key": _entity_key(
                        "|".join(
                            (
                                assertion.object.namespace_id,
                                assertion.object.source_id,
                                assertion.object.referent_kind,
                            )
                        )
                    ),
                    "object_entity_level": assertion.object.referent_kind,
                    "qualifiers_json": canonical_json(assertion.qualifiers),
                    "assertion_ids_json": canonical_json([assertion.assertion_id]),
                    "trace_json": _trace(
                        assertion.assertion_id,
                        source_product_id=source_product_id,
                        source_record_id=source_record_id,
                        source_path=assertion.provenance.source_path,
                    ),
                }
            )
    return project_gold_entity(
        {
            "entity_key": _entity_key(mbid),
            "entity_level": "MUSIC_RECORDING",
            "entity_kind": "MUSIC_RECORDING",
            "status": "ACTIVE",
            "release_plan_id": "sha256:" + ("8" * 64),
            "source_node_count": 2,
            "fields": fields,
            "identifiers": identifiers,
            "relations": relations,
            "relation_summary": [
                {"predicate": predicate, "count": count}
                for predicate, count in sorted(
                    Counter(item["predicate"] for item in relations).items()
                )
            ],
            "conflict_count": 0,
            "conflicts": [],
        }
    )


def test_real_authority_facts_generate_music_gold_match_fields() -> None:
    _cohort, musicbrainz, wikidata, _profile = _pilot_material()
    exact_documents = [
        _project_recording_coordinate(
            qid=qid,
            mbid=mbid,
            musicbrainz=musicbrainz,
            wikidata=wikidata,
        )
        for qid, mbid in sorted(EXACT_COORDINATES)
    ]
    variant_document = _project_recording_coordinate(
        qid=VARIANT_COORDINATE[0],
        mbid=VARIANT_COORDINATE[1],
        musicbrainz=musicbrainz,
        wikidata=wikidata,
    )

    assert all(item["musicExactEligible"] is True for item in exact_documents)
    assert all(item["domains"] == ["MUSIC"] for item in exact_documents)
    assert all(len(item["musicVariantSignals"]) <= 1 for item in exact_documents)
    assert all(
        signal["authorityProperty"] is None
        for item in exact_documents
        for signal in item["musicVariantSignals"]
    )
    assert all(item["musicMediaObjects"] for item in exact_documents)
    assert all(
        any(
            identifier["namespace"] == "musicbrainz-recording"
            for identifier in item["externalIdentifiers"]
        )
        for item in exact_documents
    )
    assert all(
        any(
            identifier["namespace"] == "wikidata-item"
            for identifier in item["externalIdentifiers"]
        )
        for item in exact_documents
    )
    assert any(len(item["titles"]) > 1 for item in exact_documents)
    assert any(item["musicWorkParentEntityKeys"] for item in exact_documents)
    assert any(item["musicCredits"] for item in exact_documents)
    assert {"COMPOSER", "PERFORMER"} <= {
        credit["role"]
        for document in exact_documents
        for credit in document["musicCredits"]
    }
    assert variant_document["musicExactEligible"] is False
    assert len(variant_document["musicMediaObjects"]) == 6
    assert len(variant_document["musicVariantSignals"]) == 4
    assert {
        item["authorityProperty"]
        for item in variant_document["musicVariantSignals"]
        if item["authorityProperty"] is not None
    } == set()
    assert {
        item["authorityRelation"]
        for item in variant_document["musicVariantSignals"]
        if item["authorityRelation"] is not None
    } == {"remix"}
    assert (
        "Там, за чертой (DIP Project remix full version).mp3"
        in variant_document["musicMediaObjects"]
    )
