from __future__ import annotations

import pytest
from pydantic import ValidationError

from video_media_catalog.douban import douban_jump_url, normalize_douban_id
from video_media_catalog.gold_api_models import GoldExternalIdentifier


def test_douban_jump_urls_use_distinct_fixed_paths() -> None:
    assert (
        douban_jump_url("douban-work", "1295644", "EDITORIAL_WORK")
        == "https://movie.douban.com/subject/1295644/"
    )
    assert (
        douban_jump_url("douban-person", "30123456", "AGENT")
        == "https://movie.douban.com/celebrity/30123456/"
    )
    assert (
        douban_jump_url("douban-subject", "1295644", "MOVIE")
        == "https://movie.douban.com/subject/1295644/"
    )
    assert (
        douban_jump_url("douban", "30123456", "PERSON")
        == "https://movie.douban.com/celebrity/30123456/"
    )


@pytest.mark.parametrize(
    ("namespace", "value", "referent_kind"),
    [
        ("unknown", "1295644", "EDITORIAL_WORK"),
        ("douban-work", "0", "EDITORIAL_WORK"),
        ("douban-work", "01", "EDITORIAL_WORK"),
        ("douban-work", "-1", "EDITORIAL_WORK"),
        ("douban-work", "1295644/../../admin", "EDITORIAL_WORK"),
        (
            "douban-work",
            "1295644?next=https://evil.example",
            "EDITORIAL_WORK",
        ),
        (
            "douban-work",
            "\uff11\uff12\uff19\uff15\uff16\uff14\uff14",
            "EDITORIAL_WORK",
        ),
        ("douban-work", "9" * 33, "EDITORIAL_WORK"),
        ("douban-work", "1295644", "AGENT"),
        ("douban-person", "30123456", "EDITORIAL_WORK"),
        ("douban-subject", "1295644", "ORGANIZATION"),
    ],
)
def test_douban_jump_url_fails_closed(
    namespace: str,
    value: str,
    referent_kind: str,
) -> None:
    assert douban_jump_url(namespace, value, referent_kind) is None


@pytest.mark.parametrize(
    "value",
    ["", "0", "01", "-1", "1/2", "1?x=y", "\uff11\uff12\uff13", "9" * 33],
)
def test_douban_id_normalization_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError, match="Douban ID"):
        normalize_douban_id(value)


def test_gold_api_model_accepts_only_derived_douban_url() -> None:
    identifier = GoldExternalIdentifier(
        namespace="douban-work",
        value="1295644",
        issuer="Douban",
        referent_kind="EDITORIAL_WORK",
        url="https://movie.douban.com/subject/1295644/",
    )
    assert identifier.url == "https://movie.douban.com/subject/1295644/"

    legacy_without_url = GoldExternalIdentifier(
        namespace="douban-subject",
        value="1295644",
        issuer="Douban",
        referent_kind="EDITORIAL_WORK",
    )
    assert legacy_without_url.url is None

    for namespace, value, url in (
        (
            "douban-work",
            "1295644",
            "https://movie.douban.com/celebrity/1295644/",
        ),
        ("douban-work", "1295644/../../admin", "https://evil.example/"),
        ("unknown", "1295644", "https://movie.douban.com/subject/1295644/"),
    ):
        with pytest.raises(ValidationError, match="safe derived URL"):
            GoldExternalIdentifier(
                namespace=namespace,
                value=value,
                issuer="Douban",
                referent_kind="EDITORIAL_WORK",
                url=url,
            )
