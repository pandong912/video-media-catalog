"""CLI for Global Research Catalog reference contracts and releases."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from video_media_catalog.canonical import canonical_json
from video_media_catalog.catalog_source_policy import load_catalog_source_policy
from video_media_catalog.gold import GoldReleasePlan
from video_media_catalog.gold_ingest import GoldReleaseCommit
from video_media_catalog.gold_search_index import (
    MAPPING_DIGEST,
    PROJECTION_VERSION,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldIndexBuildManifest,
    GoldIndexValidationReceipt,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.predicate_registry import (
    load_research_predicate_registry,
)
from video_media_catalog.reference_fixture import (
    load_reference_fixture,
    materialize_reference_fixture,
)
from video_media_catalog.reference_ontology import (
    load_global_reference_ontology,
)
from video_media_catalog.reference_release import (
    GLOBAL_REFERENCE_RELEASE_MEDIA_TYPE,
    GlobalReferenceReleaseManifest,
    build_global_reference_release_manifest,
)

_RELEASE_SCHEMA_PATH = (
    Path(__file__).resolve().parents[2]
    / "contracts/catalog/global-reference-release.v1.schema.json"
)
_PACKAGED_RELEASE_SCHEMA_PATH = (
    Path(__file__).resolve().parent
    / "contracts/global-reference-release.v1.schema.json"
)


def _load_json(path: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def _coordinates() -> dict[str, Any]:
    ontology = load_global_reference_ontology()
    predicates = load_research_predicate_registry()
    source_policy = load_catalog_source_policy()
    return {
        "ontology": {
            "registryId": ontology.registry_id,
            "registryVersion": ontology.registry_version,
            "digest": ontology.digest,
        },
        "predicates": {
            "registryId": predicates.registry_id,
            "registryVersion": predicates.registry_version,
            "digest": predicates.digest,
        },
        "sourcePolicy": {
            "schemaVersion": source_policy.canonical_registry.schema_version,
            "registryVersion": source_policy.canonical_registry.registry_version,
            "digest": source_policy.digest,
        },
        "searchProjection": {
            "projectionVersion": PROJECTION_VERSION,
            "mappingDigest": MAPPING_DIGEST,
            "indexPrefix": RESEARCH_INDEX_PREFIX,
            "readAlias": RESEARCH_READ_ALIAS,
        },
        "release": {
            "schemaVersion": "1.0",
            "mediaType": GLOBAL_REFERENCE_RELEASE_MEDIA_TYPE,
            "capabilities": ["DISPLAY", "IDENTITY", "RESEARCH"],
        },
    }


def _validate_fixtures(paths: list[str]) -> dict[str, Any]:
    summaries = []
    for path in paths:
        materialized = materialize_reference_fixture(load_reference_fixture(path))
        field_count = sum(len(item.field_assertions) for item in materialized.mapped)
        identifier_count = sum(
            len(item.identifier_assertions) for item in materialized.mapped
        )
        relationship_count = sum(
            len(item.relationship_assertions) for item in materialized.mapped
        )
        type_count = sum(
            len(item.entity_type_assertions) for item in materialized.mapped
        )
        summaries.append(
            {
                "fixtureId": materialized.fixture.fixture_id,
                "domain": materialized.fixture.domain,
                "fixtureDigest": materialized.fixture.digest,
                "sourceSnapshotPinId": materialized.captured.pin.pin_id,
                "captureBatchId": materialized.captured.batch_manifest.batch_id,
                "recordCount": len(materialized.captured.envelopes),
                "citationCount": len(materialized.citations),
                "assertionCounts": {
                    "fields": field_count,
                    "identifiers": identifier_count,
                    "relationships": relationship_count,
                    "entityTypes": type_count,
                },
            }
        )
    return {"fixtures": sorted(summaries, key=lambda item: item["fixtureId"])}


def _release_schema() -> dict[str, Any]:
    path = (
        _RELEASE_SCHEMA_PATH
        if _RELEASE_SCHEMA_PATH.is_file()
        else _PACKAGED_RELEASE_SCHEMA_PATH
    )
    return _load_json(str(path))


def _validate_release_manifest(path: str) -> dict[str, Any]:
    from jsonschema import Draft202012Validator

    payload = _load_json(path)
    Draft202012Validator(_release_schema()).validate(payload)
    release = GlobalReferenceReleaseManifest.model_validate(payload)
    coordinates = _coordinates()
    if (
        release.ontology_digest != coordinates["ontology"]["digest"]
        or release.predicate_registry_digest != coordinates["predicates"]["digest"]
        or release.source_policy_digest != coordinates["sourcePolicy"]["digest"]
        or release.validated_index.mapping_digest
        != coordinates["searchProjection"]["mappingDigest"]
        or release.validated_index.read_alias
        != coordinates["searchProjection"]["readAlias"]
        or not release.validated_index.concrete_index.startswith(
            f"{coordinates['searchProjection']['indexPrefix']}-"
        )
    ):
        raise ValueError("release coordinates drifted from the live registry pins")
    return {
        "releaseId": release.release_id,
        "goldReleasePlanId": release.gold_release_plan_id,
        "concreteIndex": release.validated_index.concrete_index,
        "capabilities": [item.value for item in release.capabilities],
        "ontologyDigest": release.ontology_digest,
        "predicateRegistryDigest": release.predicate_registry_digest,
        "sourcePolicyDigest": release.source_policy_digest,
        "mappingDigest": release.validated_index.mapping_digest,
    }


def _build_release(path: str) -> dict[str, Any]:
    payload = _load_json(path)
    required_bundle_keys = {
        "manifest",
        "goldReleasePlan",
        "goldReleaseCommit",
        "indexBuild",
        "indexValidation",
        "indexBuildManifest",
        "indexValidationReceipt",
    }
    if set(payload) != required_bundle_keys:
        raise ValueError(
            "release bundle must contain exactly the pinned Gold and index evidence"
        )
    manifest = payload.get("manifest")
    if not isinstance(manifest, dict):
        raise ValueError("release bundle requires a manifest object")
    allowed_manifest_keys = {
        "previousReleaseId",
        "sourceSnapshots",
        "sliceSourcePins",
        "goldReleasePlan",
        "goldReleaseCommit",
        "createdAt",
    }
    if set(manifest) - allowed_manifest_keys:
        raise ValueError("release bundle manifest contains generated or unknown fields")
    required_manifest_keys = allowed_manifest_keys - {"previousReleaseId"}
    if not required_manifest_keys <= set(manifest):
        raise ValueError("release bundle manifest is missing required release pins")
    manifest_values = {
        "previous_release_id": manifest.get("previousReleaseId"),
        "source_snapshots": manifest["sourceSnapshots"],
        "slice_source_pins": manifest["sliceSourcePins"],
        "created_at": manifest["createdAt"],
    }
    release = build_global_reference_release_manifest(
        gold_plan=GoldReleasePlan.model_validate(payload["goldReleasePlan"]),
        gold_plan_reference=ObjectRef.model_validate(manifest["goldReleasePlan"]),
        gold_commit=GoldReleaseCommit.model_validate(payload["goldReleaseCommit"]),
        gold_commit_reference=ObjectRef.model_validate(manifest["goldReleaseCommit"]),
        index_build=GoldIndexBuildManifest.model_validate(payload["indexBuild"]),
        index_validation=GoldIndexValidationReceipt.model_validate(
            payload["indexValidation"]
        ),
        index_build_manifest=ObjectRef.model_validate(payload["indexBuildManifest"]),
        index_validation_receipt=ObjectRef.model_validate(
            payload["indexValidationReceipt"]
        ),
        **manifest_values,
    )
    return release.model_dump(mode="json", by_alias=True, exclude_none=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="video-media-catalog-global-reference")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("coordinates")
    fixtures = commands.add_parser("validate-fixtures")
    fixtures.add_argument("--fixture", action="append", required=True)
    release = commands.add_parser("build-release")
    release.add_argument("--bundle", required=True)
    validate = commands.add_parser("validate-release")
    validate.add_argument("--manifest", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    if parsed.command == "coordinates":
        result = _coordinates()
    elif parsed.command == "validate-fixtures":
        result = _validate_fixtures(parsed.fixture)
    elif parsed.command == "build-release":
        result = _build_release(parsed.bundle)
    elif parsed.command == "validate-release":
        result = _validate_release_manifest(parsed.manifest)
    else:
        raise SystemExit(f"unsupported command: {parsed.command}")
    print(canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
