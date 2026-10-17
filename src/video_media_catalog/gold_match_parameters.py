"""Publish an immutable Catalog Gold match parameter object from one index build."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json, canonical_json_bytes
from video_media_catalog.catalog_source_policy import load_catalog_source_policy
from video_media_catalog.gold import RESEARCH_CONTEXT_ID
from video_media_catalog.gold_search_index import (
    MAPPING_DIGEST,
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
    GoldIndexBuildManifest,
)

PARAMETER_SCHEMA_VERSION = "1.0"
PARAMETER_MEDIA_TYPE = (
    "application/vnd.video-governance.catalog-gold-match-parameters.v1+json"
)
_CONCRETE_INDEX = re.compile(rf"^{RESEARCH_INDEX_PREFIX}-[0-9a-f]{{24}}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_REGION = re.compile(r"^[a-z]{2}-[a-z]+-\d$")
_REQUIRED_MUSIC_EXACT_NAMESPACES = frozenset(
    {
        "isrc",
        "iswc",
        "musicbrainz-artist",
        "musicbrainz-label",
        "musicbrainz-recording",
        "musicbrainz-release",
        "musicbrainz-release-group",
        "musicbrainz-work",
        "wikidata-item",
    }
)


def _music_match_policy(contract: Mapping[str, Any]) -> dict[str, Any]:
    domains = contract.get("domains")
    authority = contract.get("domainIdentityAuthority")
    policy = contract.get("musicMatchPolicy")
    supported_domains = (
        authority.get("supportedDomains") if isinstance(authority, Mapping) else None
    )
    if (
        not isinstance(domains, list)
        or "MUSIC" not in domains
        or len(domains) != len(set(domains))
        or not isinstance(supported_domains, list)
        or "MUSIC" not in supported_domains
        or not isinstance(policy, Mapping)
    ):
        raise ValueError("catalog match algorithm must declare MUSIC support")
    namespaces = policy.get("exactIdentifierNamespaces")
    parent = policy.get("parentConstrained")
    parent_predicates = (
        parent.get("predicates") if isinstance(parent, Mapping) else None
    )
    if (
        not isinstance(namespaces, list)
        or namespaces != sorted(_REQUIRED_MUSIC_EXACT_NAMESPACES)
        or not isinstance(parent, Mapping)
        or parent.get("decision") != "REVIEW_ONLY"
        or not isinstance(parent_predicates, list)
        or parent_predicates != ["recording_of", "release_membership"]
        or policy.get("ambiguousExactCandidates") != "ABSTAIN"
    ):
        raise ValueError(
            "MUSIC matching requires exact IDs, parent review, and ambiguity abstention"
        )
    return json.loads(canonical_json(dict(policy)))


def catalog_match_algorithm_identity(
    contract: Mapping[str, Any],
) -> tuple[str, str, str]:
    if contract.get("schemaVersion") != "1.0":
        raise ValueError("unsupported catalog match algorithm schema")
    algorithm_id = contract.get("algorithmId")
    if algorithm_id != "catalog-asset-match-v1":
        raise ValueError("unsupported catalog match algorithm ID")
    _music_match_policy(contract)
    digest = (
        "sha256:" + hashlib.sha256(canonical_json_bytes(dict(contract))).hexdigest()
    )
    return str(algorithm_id), str(contract["schemaVersion"]), digest


def match_parameters_from_manifest(
    manifest: GoldIndexBuildManifest,
    *,
    algorithm_contract: Mapping[str, Any],
    search_endpoint: str,
    aws_region: str,
    aws_service: str = "es",
    top_k: int = 20,
) -> dict[str, Any]:
    """Pin one completed research index build. Read aliases are rejected."""

    if manifest.context_id != RESEARCH_CONTEXT_ID:
        raise ValueError("match parameters require contextId=research")
    if manifest.alias != RESEARCH_READ_ALIAS:
        raise ValueError("match parameters require the research read alias")
    concrete = _CONCRETE_INDEX.fullmatch(manifest.index)
    if manifest.index == manifest.alias or concrete is None:
        raise ValueError("concreteIndex must be a 24-hex research index, not an alias")
    if manifest.mapping_digest != MAPPING_DIGEST:
        raise ValueError("index manifest mappingDigest does not match projection v10")
    if manifest.source_policy_digest != load_catalog_source_policy().digest:
        raise ValueError(
            "index manifest sourcePolicyDigest does not match canonical policy"
        )
    if not _SHA256.fullmatch(manifest.config_digest):
        raise ValueError("configDigest must be sha256")
    if not _SHA256.fullmatch(manifest.release_plan_id):
        raise ValueError("releasePlanId must be sha256")
    endpoint = urlsplit(search_endpoint)
    if (
        endpoint.scheme != "https"
        or not endpoint.hostname
        or endpoint.username is not None
        or endpoint.password is not None
        or endpoint.query
        or endpoint.fragment
    ):
        raise ValueError("searchEndpoint must be a private HTTPS origin")
    if endpoint.hostname == "example.invalid" or endpoint.hostname.endswith(
        ".example.invalid"
    ):
        raise ValueError("searchEndpoint must not use a placeholder host")
    if aws_service != "es" or _REGION.fullmatch(aws_region) is None:
        raise ValueError("match parameters require the es service and an AWS region")
    if isinstance(top_k, bool) or not isinstance(top_k, int) or not 1 <= top_k <= 20:
        raise ValueError("topK must be from 1 to 20")
    algorithm_id, algorithm_schema, algorithm_digest = catalog_match_algorithm_identity(
        algorithm_contract
    )
    music_policy = _music_match_policy(algorithm_contract)
    return {
        "schemaVersion": PARAMETER_SCHEMA_VERSION,
        "contextId": RESEARCH_CONTEXT_ID,
        "releasePlanId": manifest.release_plan_id,
        "concreteIndex": manifest.index,
        "mappingDigest": MAPPING_DIGEST,
        "configDigest": manifest.config_digest,
        "algorithmId": algorithm_id,
        "algorithmSchemaVersion": algorithm_schema,
        "algorithmDigest": algorithm_digest,
        "supportedDomains": list(algorithm_contract["domains"]),
        "musicMatchPolicy": music_policy,
        "searchEndpoint": search_endpoint.rstrip("/"),
        "awsRegion": aws_region,
        "awsService": aws_service,
        "topK": top_k,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument(
        "--algorithm-contract",
        type=Path,
        required=True,
        help="Canonical contract from video-world-compute",
    )
    parser.add_argument("--search-endpoint", required=True)
    parser.add_argument("--aws-region", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--top-k", type=int, default=20)
    args = parser.parse_args(argv)
    manifest = GoldIndexBuildManifest.model_validate_json(
        args.manifest.read_text(encoding="utf-8")
    )
    algorithm_contract = json.loads(args.algorithm_contract.read_text(encoding="utf-8"))
    if not isinstance(algorithm_contract, dict):
        raise ValueError("algorithm contract must contain a JSON object")
    document = match_parameters_from_manifest(
        manifest,
        algorithm_contract=algorithm_contract,
        search_endpoint=args.search_endpoint,
        aws_region=args.aws_region,
        top_k=args.top_k,
    )
    args.output.write_text(canonical_json(document) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
