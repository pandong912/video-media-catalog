"""Render the reviewed source registry and immutable rights-policy digests."""

from __future__ import annotations

import argparse

from video_media_catalog.canonical import canonical_json
from video_media_catalog.community_sources import build_community_registry


def build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        prog="video-media-catalog-source-registry",
        description="Print the complete reviewed v2 source registry snapshot.",
    )


def run(_: argparse.Namespace) -> dict[str, object]:
    registry = build_community_registry()
    return {
        "registryDigest": registry.digest,
        "registry": registry.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        ),
    }


def main() -> None:
    print(canonical_json(run(build_parser().parse_args())))


if __name__ == "__main__":
    main()
