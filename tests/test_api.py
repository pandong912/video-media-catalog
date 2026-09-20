from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from video_media_catalog.api import APISettings, create_app
from video_media_catalog.api_auth import Principal
from video_media_catalog.api_search import select_description

ENTITY_KEY = "sha256:" + "1" * 64


class FakeTransport:
    def __init__(self, owner: FakeOpenSearch) -> None:
        self.owner = owner

    def perform_request(self, **kwargs: Any) -> dict[str, Any]:
        self.owner.search_requests.append(kwargs)
        if self.owner.search_responses:
            return self.owner.search_responses.pop(0)
        return {
            "timed_out": False,
            "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []},
        }


class FakeOpenSearch:
    def __init__(self) -> None:
        self.search_responses: list[dict[str, Any]] = []
        self.search_requests: list[dict[str, Any]] = []
        self.get_requests: list[dict[str, Any]] = []
        self.transport = FakeTransport(self)
        self.entity = {
            "entityKey": ENTITY_KEY,
            "entityType": "MOVIE",
            "canonicalSource": "wikidata",
            "canonicalSourceId": "Q1",
            "displayName": "Example",
            "displayLanguage": "en",
            "names": [],
            "descriptions": [
                {"language": "en", "value": "English description"},
                {"language": "zh-hans", "value": "简体描述"},
            ],
            "sitelinks": [],
            "attributes": {
                "releaseDates": [],
                "durations": [],
                "languages": [],
                "countries": [],
                "genres": [],
                "episodeCounts": [],
                "seasonCounts": [],
                "modified": [],
            },
            "externalIdentifiers": [
                {
                    "scheme": f"scheme-{index}",
                    "value": f"value-{index}",
                    "source": "wikidata",
                    "sourceRecordId": "Q1",
                }
                for index in range(6)
            ],
            "relations": [],
            "relationSummary": [],
            "parentKeys": [],
            "sourceRecordIds": ["Q1"],
            "sourceRecords": [],
        }
        self.gold_entity = {
            "entityKey": ENTITY_KEY,
            "entityLevel": "SERIES",
            "entityKind": "TV_SERIES",
            "status": "ACTIVE",
            "releasePlanId": "sha256:" + ("2" * 64),
            "contextId": "personal-research",
            "displayName": "Gold Example",
            "displayLanguage": "en",
            "titles": [
                {
                    "value": "Gold Example",
                    "language": "en",
                    "titleRole": "PRIMARY",
                }
            ],
            "attributes": {
                "formats": ["Scripted"],
                "languages": ["English"],
                "statuses": ["Running"],
                "premiered": ["2020-01-01"],
                "ended": [],
                "runtimeMinutes": ["45"],
                "averageRuntimeMinutes": ["45"],
                "genres": ["Drama"],
            },
            "externalIdentifiers": [
                {
                    "namespace": "imdb-title",
                    "value": "tt0000001",
                    "issuer": "IMDb",
                    "referentKind": "SERIES",
                }
            ],
            "relationSummary": [],
            "sourceBadges": [
                {
                    "sourceProductId": "tvmaze-public-api",
                    "displayName": "TVmaze public API",
                    "sourceUrl": "https://www.tvmaze.com/api",
                    "policyZones": ["open_sharealike"],
                    "assertionCount": 1,
                    "winningAssertionCount": 1,
                }
            ],
            "winningAssertions": [
                {
                    "kind": "FIELD",
                    "assertionId": "sha256:" + ("3" * 64),
                    "predicate": "title",
                    "valueJson": '"Gold Example"',
                    "qualifiersJson": "{}",
                    "resolutionStatus": "SELECTED",
                    "sourceProductId": "tvmaze-public-api",
                    "sourceRecordId": "1",
                    "sourcePath": "/name",
                    "observedAt": "2026-09-19T00:00:00Z",
                    "citationKeys": [],
                    "citationOverflow": 0,
                }
            ],
            "rights": [
                {
                    "sourceProductId": "tvmaze-public-api",
                    "policyId": "tvmaze-api-cc-by-sa",
                    "policyZone": "open_sharealike",
                    "licenseId": "CC-BY-SA",
                    "licenseUri": None,
                    "attributionText": "TV data provided by TVmaze.",
                    "sourceUrl": "https://www.tvmaze.com/api",
                    "shareAlike": True,
                }
            ],
            "conflictCount": 0,
            "conflictPredicates": [],
            "conflicts": [],
            "sourceNodeCount": 1,
            "overflow": {
                "titles": 0,
                "externalIdentifiers": 0,
                "relationTypes": 0,
                "sourceBadges": 0,
                "winningAssertions": 0,
                "citationKeys": 0,
                "rights": 0,
                "conflicts": 0,
                "formats": 0,
                "languages": 0,
                "statuses": 0,
                "premiered": 0,
                "ended": 0,
                "runtimeMinutes": 0,
                "averageRuntimeMinutes": 0,
                "genres": 0,
            },
        }

    def get(self, **kwargs: Any) -> dict[str, Any]:
        self.get_requests.append(kwargs)
        return {
            "_source": (
                self.gold_entity
                if kwargs.get("index") == "media-catalog-research-read"
                else self.entity
            )
        }


class AcceptingVerifier:
    def __init__(
        self,
        *,
        subject: str = "user-1",
        scopes: frozenset[str] = frozenset({"governance.read"}),
    ) -> None:
        self.tokens: list[str] = []
        self.subject = subject
        self.scopes = scopes

    def verify(self, token: str) -> Principal:
        self.tokens.append(token)
        return Principal(
            subject=self.subject,
            scopes=self.scopes,
        )


def _test_settings() -> APISettings:
    return APISettings(
        opensearch_endpoint="https://search.example",
        cursor_secret="test-cursor-secret-" + "x" * 32,
        aws_region="us-east-1",
        environment="test",
        auth_disabled=True,
    )


def authenticated_settings() -> APISettings:
    return APISettings(
        opensearch_endpoint="https://search.example",
        cursor_secret="test-cursor-secret-" + "x" * 32,
        aws_region="us-east-1",
        environment="test",
        oidc_issuer="https://issuer.example",
        oidc_jwks_uri="https://issuer.example/jwks.json",
        oidc_audience="media-catalog-api",
        oidc_owner_subject="user-1",
    )


def test_production_settings_fail_closed_without_oidc() -> None:
    with pytest.raises(ValueError, match="OIDC"):
        APISettings(
            opensearch_endpoint="https://search.example",
            cursor_secret="x" * 32,
            aws_region="us-east-1",
        )


def test_settings_require_one_owner_subject_when_auth_is_enabled() -> None:
    with pytest.raises(ValueError, match="owner subject"):
        APISettings(
            opensearch_endpoint="https://search.example",
            cursor_secret="x" * 32,
            aws_region="us-east-1",
            oidc_issuer="https://issuer.example",
            oidc_jwks_uri="https://issuer.example/jwks.json",
            oidc_audience="media-catalog-api",
        )


def test_settings_accept_infrastructure_environment_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENSEARCH_ENDPOINT", "https://search.example")
    monkeypatch.setenv("REGION", "us-east-1")
    monkeypatch.setenv("INDEX_ALIAS", "media-catalog-entities-read")
    monkeypatch.setenv("OIDC_ISSUER", "https://issuer.example/oidc")
    monkeypatch.setenv(
        "OIDC_JWKS_URI",
        "http://logto.logto.svc.cluster.local:3001/oidc/jwks",
    )
    monkeypatch.setenv("OIDC_AUDIENCE", "media-catalog-api")
    monkeypatch.setenv("OIDC_OWNER_SUBJECT", "owner-123")
    monkeypatch.setenv("OIDC_REQUIRED_SCOPE", "catalog.read")
    monkeypatch.setenv("MEDIA_CATALOG_CURSOR_SECRET", "x" * 32)

    settings = APISettings.from_env()

    assert settings.opensearch_endpoint == "https://search.example"
    assert settings.oidc_required_scope == "catalog.read"
    assert settings.oidc_owner_subject == "owner-123"
    assert settings.oidc_config().required_scope == "catalog.read"


def test_health_is_public_while_catalog_requires_bearer() -> None:
    verifier = AcceptingVerifier()
    app = create_app(
        authenticated_settings(),
        client=FakeOpenSearch(),
        verifier=verifier,
    )

    with TestClient(app) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        denied = client.get("/api/v1/catalog/search", params={"q": "film"})
        accepted = client.get(
            "/api/v1/catalog/search",
            params={"q": "film"},
            headers={"Authorization": "Bearer signed-token"},
        )

    assert denied.status_code == 401
    assert denied.headers["content-type"].startswith("application/problem+json")
    assert denied.json()["code"] == "AUTHENTICATION_REQUIRED"
    assert denied.json()["retryable"] is False
    assert accepted.status_code == 200
    assert verifier.tokens == ["signed-token"]


@pytest.mark.parametrize(
    ("verifier", "expected_code"),
    [
        (AcceptingVerifier(subject="another-user"), "OWNER_ONLY"),
        (
            AcceptingVerifier(scopes=frozenset({"profile"})),
            "INSUFFICIENT_SCOPE",
        ),
    ],
)
def test_v2_research_routes_require_scope_and_exact_owner(
    verifier: AcceptingVerifier,
    expected_code: str,
) -> None:
    app = create_app(
        authenticated_settings(),
        client=FakeOpenSearch(),
        verifier=verifier,
    )

    with TestClient(app) as client:
        denied = client.get(
            "/api/v2/research/search",
            headers={"Authorization": "Bearer signed-token"},
        )

    assert denied.status_code == 403
    assert denied.json()["code"] == expected_code


def test_openapi_documents_bearer_security_and_public_health() -> None:
    verifier = AcceptingVerifier()
    app = create_app(
        authenticated_settings(),
        client=FakeOpenSearch(),
        verifier=verifier,
    )

    schema = app.openapi()
    with TestClient(app) as client:
        denied = client.get("/openapi.json")
        accepted = client.get(
            "/openapi.json",
            headers={"Authorization": "Bearer signed-token"},
        )

    assert "security" not in schema["paths"]["/healthz"]["get"]
    search_operation = schema["paths"]["/api/v1/catalog/search"]["get"]
    assert search_operation["security"]
    research_operation = schema["paths"]["/api/v2/research/search"]["get"]
    assert research_operation["security"]
    assert "/api/v2/catalog/search" not in schema["paths"]
    assert schema["components"]["securitySchemes"]["HTTPBearer"]["scheme"] == "bearer"
    assert denied.status_code == 401
    assert accepted.status_code == 200


def test_search_builds_fixed_query_and_signed_search_after_cursor() -> None:
    search = FakeOpenSearch()
    hit = {
        "_source": search.entity,
        "_score": 2.5,
        "sort": [2.5, ENTITY_KEY],
    }
    search.search_responses = [
        {
            "timed_out": False,
            "hits": {
                "total": {"value": 200, "relation": "gte"},
                "hits": [hit],
            },
        },
        {
            "timed_out": False,
            "hits": {
                "total": {"value": 200, "relation": "gte"},
                "hits": [],
            },
        },
    ]
    app = create_app(_test_settings(), client=search)

    with TestClient(app) as client:
        first = client.get(
            "/api/v1/catalog/search",
            params={
                "q": 'title:example OR "*"',
                "entityType": "MOVIE",
                "language": "ZH_HANS",
                "pageSize": 1,
            },
        )
        cursor = first.json()["nextCursor"]
        second = client.get(
            "/api/v1/catalog/search",
            params={
                "q": 'title:example OR "*"',
                "entityType": "MOVIE",
                "language": "zh-hans",
                "pageSize": 1,
                "cursor": cursor,
            },
        )

    assert first.status_code == 200
    assert first.json()["totalValue"] == 200
    assert first.json()["totalRelation"] == "gte"
    assert set(first.json()["items"][0]) == {
        "entityKey",
        "entityType",
        "displayName",
        "displayLanguage",
        "description",
        "externalIdentifiers",
    }
    assert first.json()["items"][0]["description"] == "简体描述"
    assert len(first.json()["items"][0]["externalIdentifiers"]) == 5
    assert second.status_code == 200
    first_body = search.search_requests[0]["body"]
    assert "query_string" not in str(first_body)
    assert first_body["query"]["bool"]["filter"][0] == {"term": {"entityType": "MOVIE"}}
    assert search.search_requests[1]["body"]["search_after"] == [2.5, ENTITY_KEY]
    assert all(request["method"] == "GET" for request in search.search_requests)
    assert all(
        request["url"] == "/media-catalog-entities-read/_search"
        for request in search.search_requests
    )


def test_cursor_tampering_and_cross_query_reuse_are_rejected() -> None:
    search = FakeOpenSearch()
    search.search_responses = [
        {
            "timed_out": False,
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {
                        "_source": search.entity,
                        "sort": [1.0, ENTITY_KEY],
                    }
                ],
            },
        }
    ]
    app = create_app(_test_settings(), client=search)

    with TestClient(app) as client:
        first = client.get(
            "/api/v1/catalog/search",
            params={"q": "film", "pageSize": 1},
        )
        cursor = first.json()["nextCursor"]
        replacement = "A" if cursor[-1] != "A" else "B"
        tampered = client.get(
            "/api/v1/catalog/search",
            params={
                "q": "film",
                "pageSize": 1,
                "cursor": cursor[:-1] + replacement,
            },
        )
        rebound = client.get(
            "/api/v1/catalog/search",
            params={"q": "series", "pageSize": 1, "cursor": cursor},
        )

    assert tampered.status_code == 400
    assert rebound.status_code == 400
    assert len(search.search_requests) == 1


@pytest.mark.parametrize(
    ("requested", "display", "expected"),
    [
        ("fr", "en", "Français"),
        ("de", "en", "English"),
        ("de", "ja", "简体"),
        (None, "ja", "简体"),
    ],
)
def test_description_language_fallback(
    requested: str | None,
    display: str,
    expected: str,
) -> None:
    descriptions = [
        {"language": "mul", "value": "Universal"},
        {"language": "zh-hans", "value": "简体"},
        {"language": "en", "value": "English"},
        {"language": "fr", "value": "Français"},
    ]

    assert (
        select_description(
            descriptions,
            requested_language=requested,
            display_language=display,
        )
        == expected
    )


def test_description_falls_back_to_first_available_value() -> None:
    assert (
        select_description(
            [{"language": "ja", "value": "最初"}, {"language": "ko", "value": "둘째"}],
            requested_language="de",
            display_language="fr",
        )
        == "最初"
    )


def test_empty_or_missing_query_browses_with_match_all() -> None:
    search = FakeOpenSearch()
    app = create_app(_test_settings(), client=search)

    with TestClient(app) as client:
        missing = client.get("/api/v1/catalog/search")
        empty = client.get("/api/v1/catalog/search", params={"q": ""})

    assert missing.status_code == 200
    assert empty.status_code == 200
    assert missing.json() == {
        "items": [],
        "nextCursor": None,
        "totalValue": 0,
        "totalRelation": "eq",
    }
    for request in search.search_requests:
        assert request["method"] == "GET"
        assert request["body"]["query"]["bool"]["must"] == [{"match_all": {}}]


def test_entity_detail_and_external_identifier_endpoints() -> None:
    search = FakeOpenSearch()
    search.search_responses = [
        {
            "timed_out": False,
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [{"_source": search.entity, "sort": [ENTITY_KEY]}],
            },
        },
        {
            "timed_out": False,
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [{"_source": search.entity, "sort": [ENTITY_KEY]}],
            },
        },
    ]
    app = create_app(_test_settings(), client=search)

    with TestClient(app) as client:
        detail = client.get(f"/api/v1/catalog/entities/{ENTITY_KEY}")
        external = client.get("/api/v1/catalog/external-identifiers/imdb/tt0000001")
        eidr = client.get(
            "/api/v1/catalog/external-identifiers/eidr/"
            "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"
        )

    assert detail.status_code == 200
    assert detail.json()["entityKey"] == ENTITY_KEY
    assert search.get_requests[0]["index"] == "media-catalog-entities-read"
    assert external.status_code == 200
    assert external.json()["entityKey"] == ENTITY_KEY
    assert "items" not in external.json()
    filters = search.search_requests[0]["body"]["query"]["nested"]["query"]["bool"][
        "filter"
    ]
    assert filters == [
        {"term": {"externalIdentifiers.scheme": "imdb"}},
        {"term": {"externalIdentifiers.value": "tt0000001"}},
    ]
    assert eidr.status_code == 200
    eidr_filters = search.search_requests[1]["body"]["query"]["nested"]["query"][
        "bool"
    ]["filter"]
    assert eidr_filters[1] == {
        "term": {"externalIdentifiers.value": "10.5240/AAAA-BBBB-CCCC-DDDD-EEEE-C"}
    }
    assert all(request["method"] == "GET" for request in search.search_requests)


def test_external_identifier_zero_and_multiple_results() -> None:
    missing = FakeOpenSearch()
    duplicate = FakeOpenSearch()
    duplicate.search_responses = [
        {
            "timed_out": False,
            "hits": {
                "total": {"value": 2, "relation": "eq"},
                "hits": [
                    {"_source": duplicate.entity},
                    {"_source": duplicate.entity},
                ],
            },
        }
    ]

    with TestClient(create_app(_test_settings(), client=missing)) as client:
        not_found = client.get("/api/v1/catalog/external-identifiers/imdb/tt404")
    with TestClient(create_app(_test_settings(), client=duplicate)) as client:
        conflict = client.get("/api/v1/catalog/external-identifiers/imdb/tt0000001")

    assert not_found.status_code == 404
    assert not_found.json()["code"] == "NOT_FOUND"
    assert conflict.status_code == 409
    assert conflict.json()["code"] == "CONFLICT"


def test_invalid_page_size_uses_problem_details() -> None:
    app = create_app(_test_settings(), client=FakeOpenSearch())

    with TestClient(app) as client:
        response = client.get(
            "/api/v1/catalog/search",
            params={"q": "film", "pageSize": 101},
        )
        unknown = client.get(
            "/api/v1/catalog/search",
            params={"q": "film", "dsl": '{"match_all":{}}'},
        )
        duplicate = client.get(
            "/api/v1/catalog/search?q=film&q=series",
        )

    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["status"] == 422
    assert response.json()["code"] == "INVALID_REQUEST"
    assert response.json()["retryable"] is False
    assert unknown.status_code == 422
    assert duplicate.status_code == 422


def test_research_search_cursor_stays_on_concrete_index() -> None:
    search = FakeOpenSearch()
    concrete_index = "media-catalog-research-" + ("a" * 24)
    search.search_responses = [
        {
            "timed_out": False,
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [
                    {
                        "_index": concrete_index,
                        "_source": search.gold_entity,
                        "_score": 2.5,
                        "sort": [2.5, ENTITY_KEY],
                    }
                ],
            },
        },
        {
            "timed_out": False,
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [],
            },
        },
    ]
    with TestClient(create_app(_test_settings(), client=search)) as client:
        first = client.get(
            "/api/v2/research/search",
            params={
                "q": "Gold",
                "entityLevel": "SERIES",
                "hasConflicts": "false",
                "pageSize": 1,
            },
        )
        second = client.get(
            "/api/v2/research/search",
            params={
                "q": "Gold",
                "entityLevel": "SERIES",
                "hasConflicts": "false",
                "pageSize": 1,
                "cursor": first.json()["nextCursor"],
            },
        )

    assert first.status_code == 200
    assert first.json()["items"][0]["displayName"] == "Gold Example"
    assert second.status_code == 200
    assert (
        search.search_requests[0]["url"]
        == "/media-catalog-research-read/_search"
    )
    assert search.search_requests[1]["url"] == f"/{concrete_index}/_search"
    assert search.search_requests[1]["body"]["search_after"] == [
        2.5,
        ENTITY_KEY,
    ]


def test_research_detail_and_external_identifier_use_research_alias() -> None:
    search = FakeOpenSearch()
    search.search_responses = [
        {
            "timed_out": False,
            "hits": {
                "total": {"value": 1, "relation": "eq"},
                "hits": [{"_source": search.gold_entity}],
            },
        }
    ]
    with TestClient(create_app(_test_settings(), client=search)) as client:
        detail = client.get(f"/api/v2/research/entities/{ENTITY_KEY}")
        external = client.get(
            "/api/v2/research/external-identifiers/imdb-title/tt0000001"
        )

    assert detail.status_code == 200
    assert detail.json()["releasePlanId"].startswith("sha256:")
    assert detail.json()["contextId"] == "personal-research"
    assert detail.json()["sourceBadges"][0]["sourceProductId"] == (
        "tvmaze-public-api"
    )
    assert detail.json()["rights"][0]["attributionText"].startswith("TV data")
    assert search.get_requests[0]["index"] == "media-catalog-research-read"
    assert external.status_code == 200
    filters = search.search_requests[0]["body"]["query"]["nested"]["query"]["bool"][
        "filter"
    ]
    assert filters == [
        {"term": {"externalIdentifiers.namespace": "imdb-title"}},
        {"term": {"externalIdentifiers.value": "tt0000001"}},
    ]
