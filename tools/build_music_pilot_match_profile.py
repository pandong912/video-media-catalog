#!/usr/bin/env python3
"""Rebuild the governed Music Pilot concrete-index match profile."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from video_media_catalog.music_pilot import (
    build_music_pilot_match_profile,
    load_music_pilot_cohort,
)
from video_media_catalog.reference_fixture import (
    load_reference_fixture,
    materialize_reference_fixture,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture-root",
        type=Path,
        default=Path("tests/fixtures/research"),
    )
    parser.add_argument(
        "--algorithm-contract",
        type=Path,
        default=Path("tests/fixtures/catalog-gold-match-algorithm.v1.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("tests/fixtures/research/music-pilot-match-profile.v1.json"),
    )
    args = parser.parse_args()

    profile = build_music_pilot_match_profile(
        cohort=load_music_pilot_cohort(
            args.fixture_root / "music-pilot-cohort.v1.json"
        ),
        musicbrainz=materialize_reference_fixture(
            load_reference_fixture(
                args.fixture_root / "musicbrainz-music-pilot.v1.json"
            )
        ),
        wikidata=materialize_reference_fixture(
            load_reference_fixture(args.fixture_root / "wikidata-music-pilot.v1.json")
        ),
        algorithm_contract=json.loads(
            args.algorithm_contract.read_text(encoding="utf-8")
        ),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            profile.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            ),
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(profile.profile_id)


if __name__ == "__main__":
    main()
