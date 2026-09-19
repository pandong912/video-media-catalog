from __future__ import annotations

import pytest

from video_media_catalog.object_store import S3Location
from video_media_catalog.reference_selection import (
    DEFAULT_AGENT_LIMITS,
    DEFAULT_CONTENT_QUOTAS,
)
from video_media_catalog.reference_subset_cli import (
    _read_demand_profile,
    build_parser,
)
from video_media_catalog.wikidata_subset_cli import _temporary_spark_locations


def _parse(*extra: str):
    return build_parser().parse_args(
        [
            "--dump-uri",
            "s3://catalog/wikidata-20260901-all.json.bz2",
            "--dump-sha256",
            "a" * 64,
            "--dump-size",
            "100",
            "--dump-version",
            "version-1",
            "--dump-etag",
            "etag",
            "--output-prefix",
            "s3://catalog/reference",
            "--staging-prefix",
            "s3://catalog/staging",
            *extra,
        ]
    )


def test_reference_subset_cli_defaults_to_content_and_agent_budgets() -> None:
    parsed = _parse()
    assert parsed.target_count == 100_000
    assert parsed.movie_count == DEFAULT_CONTENT_QUOTAS["MOVIE"]
    assert parsed.tv_series_count == DEFAULT_CONTENT_QUOTAS["TV_SERIES"]
    assert parsed.tv_season_count == DEFAULT_CONTENT_QUOTAS["TV_SEASON"]
    assert parsed.tv_episode_count == DEFAULT_CONTENT_QUOTAS["TV_EPISODE"]
    assert parsed.person_limit == DEFAULT_AGENT_LIMITS["PERSON"]
    assert parsed.organization_limit == DEFAULT_AGENT_LIMITS["ORGANIZATION"]


def test_reference_subset_cli_requires_complete_demand_profile_object_ref() -> None:
    parsed = _parse(
        "--demand-profile-uri",
        "s3://catalog/demand/profile.json",
    )
    with pytest.raises(ValueError, match="all demand-profile"):
        _read_demand_profile(
            parsed,
            s3=object(),
            store=object(),
        )


def test_spark_output_does_not_share_path_with_bfs_scratch() -> None:
    temporary = S3Location(
        "catalog",
        "raw/wikidata/subsets/_temporary/spark-output-run-1",
    )

    output, scratch_uri = _temporary_spark_locations(temporary)

    assert output.uri == (
        "s3://catalog/raw/wikidata/subsets/_temporary/spark-output-run-1/output"
    )
    assert scratch_uri == (
        "s3a://catalog/raw/wikidata/subsets/_temporary/"
        "spark-output-run-1/bfs-materialize"
    )
    assert output.uri != scratch_uri.replace("s3a://", "s3://", 1)
