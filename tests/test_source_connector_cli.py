from __future__ import annotations

import pytest

from video_media_catalog.imdb_sync_cli import build_parser as imdb_parser
from video_media_catalog.official_http import OfficialHttpsDownloader
from video_media_catalog.source_registry_cli import (
    build_parser as registry_parser,
)
from video_media_catalog.source_registry_cli import (
    run as registry_run,
)
from video_media_catalog.tmdb_sync_cli import build_parser as tmdb_parser
from video_media_catalog.v1_adapter_cli import (
    _input_object,
)
from video_media_catalog.v1_adapter_cli import (
    build_parser as adapter_parser,
)


def test_v1_adapter_cli_requires_s3_version_and_etag() -> None:
    parsed = adapter_parser().parse_args(
        [
            "--source",
            "eidr",
            "--input-uri",
            "s3://bucket/eidr.xml",
            "--input-hash",
            "a" * 64,
            "--input-size",
            "100",
            "--destination-prefix",
            "s3://bucket/output",
            "--coverage-id",
            "discovered-ids",
            "--image-digest",
            "sha256:" + ("b" * 64),
        ]
    )
    with pytest.raises(ValueError, match="version"):
        _input_object(parsed)


def test_imdb_and_tmdb_entrypoints_expose_explicit_acquisition_modes() -> None:
    imdb = imdb_parser()
    assert imdb.prog == "video-media-catalog-imdb-sync"
    tmdb = tmdb_parser().parse_args(
        [
            "changes",
            "--window-start",
            "2026-09-19",
            "--window-end",
            "2026-09-20",
            "--destination-prefix",
            "file:///tmp/tmdb",
            "--image-digest",
            "sha256:" + ("a" * 64),
            "--user-agent",
            "video-media-catalog/test",
        ]
    )
    assert tmdb.mode == "changes"
    assert not hasattr(tmdb, "read_token")


def test_source_registry_cli_emits_digest_and_private_products() -> None:
    result = registry_run(registry_parser().parse_args([]))
    assert str(result["registryDigest"]).startswith("sha256:")
    products = {
        item["sourceProductId"] for item in result["registry"]["sourceProducts"]
    }
    assert "imdb-non-commercial-datasets" in products
    assert "tmdb-personal-research" in products


def test_official_dataset_downloader_rejects_website_scraping_urls() -> None:
    downloader = OfficialHttpsDownloader(
        allowed_host="datasets.imdbws.com",
        allowed_path_prefix="/",
        user_agent="video-media-catalog/test",
    )
    assert (
        downloader.validate_url("https://datasets.imdbws.com/title.basics.tsv.gz")
        == "https://datasets.imdbws.com/title.basics.tsv.gz"
    )
    with pytest.raises(ValueError, match="official origin"):
        downloader.validate_url("https://www.imdb.com/title/tt0000001/")
