"""Publish an immutable Catalog Gold match parameter object from one index build."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.canonical import canonical_json
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
ALGORITHM_DIGEST = (
    "sha256:f95a54f39fd21fc552df883c85411c607af6c7e3b0af0c3b92a933b1f6132123"
)
_CONCRETE_INDEX = re.compile(rf"^{RESEARCH_INDEX_PREFIX}-[0-9a-f]{{24}}$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
_REGION = re.compile(r"^[a-z]{2}-[a-z]+-\d$")


def match_parameters_from_manifest(
    manifest: GoldIndexBuildManifest,
    *,
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
        raise ValueError("index manifest mappingDigest does not match projection v7")
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
    return {
        "schemaVersion": PARAMETER_SCHEMA_VERSION,
        "contextId": RESEARCH_CONTEXT_ID,
        "releasePlanId": manifest.release_plan_id,
        "concreteIndex": manifest.index,
        "mappingDigest": MAPPING_DIGEST,
        "configDigest": manifest.config_digest,
        "algorithmDigest": ALGORITHM_DIGEST,
        "searchEndpoint": search_endpoint.rstrip("/"),
        "awsRegion": aws_region,
        "awsService": aws_service,
        "topK": top_k,
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--search-endpoint", required=True)
    parser.add_argument("--aws-region", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--top-k", type=int, default=20)
    args = parser.parse_args(argv)
    manifest = GoldIndexBuildManifest.model_validate_json(
        args.manifest.read_text(encoding="utf-8")
    )
    document = match_parameters_from_manifest(
        manifest,
        search_endpoint=args.search_endpoint,
        aws_region=args.aws_region,
        top_k=args.top_k,
    )
    args.output.write_text(canonical_json(document) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
