"""FastAPI read-only service over the immutable OpenSearch read alias."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Annotated, Any
from urllib.parse import quote

from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Path,
    Query,
    Request,
    Security,
)
from fastapi.exceptions import RequestValidationError, ResponseValidationError
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.exceptions import HTTPException as StarletteHTTPException

from video_media_catalog.api_auth import (
    REQUIRED_SCOPE,
    AuthenticationError,
    AuthorizationError,
    OIDCConfig,
    OIDCJWTVerifier,
    OwnerAuthorizationError,
    Principal,
    ResearchScopeAuthorizationError,
    TokenVerifier,
    authorize_research_principal,
)
from video_media_catalog.api_models import (
    CatalogEntity,
    HealthResponse,
    ProblemDetails,
    SearchResponse,
)
from video_media_catalog.api_search import (
    CursorCodec,
    InvalidCursor,
    SearchParameters,
    build_external_identifier_query,
    build_search_query,
    select_description,
)
from video_media_catalog.gold_api_models import (
    GoldCatalogEntity,
    GoldSearchResponse,
)
from video_media_catalog.gold_api_search import (
    GoldCursorCodec,
    GoldSearchParameters,
    build_gold_external_identifier_query,
    build_gold_search_query,
)
from video_media_catalog.gold_search_index import (
    RESEARCH_INDEX_PREFIX,
    RESEARCH_READ_ALIAS,
)
from video_media_catalog.opensearch_client import (
    OpenSearchConnection,
    create_opensearch_client,
)
from video_media_catalog.search_index import READ_ALIAS
from video_media_catalog.v2_contracts import require_oidc_subject

_LANGUAGE = re.compile(r"^[a-z]{2,8}(?:-[a-z0-9]{1,8})*$")
_CONTROL_CHARACTER = re.compile(r"[\x00-\x1f\x7f]")
_ENTITY_KEY = r"^sha256:[0-9a-f]{64}$"
_SCHEME = r"^[a-z][a-z0-9._-]{0,31}$"
_ENTITY_CLASS = r"^[A-Z][A-Z0-9_]{0,63}$"
_BEARER_DEPENDENCY = Security(HTTPBearer(auto_error=False))
_AUTHENTICATED_ERRORS = {
    400: {"model": ProblemDetails},
    401: {"model": ProblemDetails},
    403: {"model": ProblemDetails},
    422: {"model": ProblemDetails},
    502: {"model": ProblemDetails},
    504: {"model": ProblemDetails},
}
_PROBLEM_DEFAULTS = {
    400: ("INVALID_REQUEST", False),
    401: ("AUTHENTICATION_REQUIRED", False),
    403: ("INSUFFICIENT_SCOPE", False),
    404: ("NOT_FOUND", False),
    409: ("CONFLICT", False),
    422: ("INVALID_REQUEST", False),
    500: ("INTERNAL_ERROR", True),
    502: ("UPSTREAM_FAILURE", True),
    504: ("UPSTREAM_TIMEOUT", True),
}
_SUMMARY_IDENTIFIER_LIMIT = 5
_SUMMARY_SOURCE_BADGE_LIMIT = 5


def _environment_bool(name: str, *, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _environment_value(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name)
        if value is not None:
            return value
    return default


@dataclass(frozen=True)
class APISettings:
    opensearch_endpoint: str
    cursor_secret: str = field(repr=False)
    aws_region: str | None = None
    opensearch_service: str = "es"
    read_alias: str = READ_ALIAS
    research_read_alias: str = RESEARCH_READ_ALIAS
    research_index_prefix: str = RESEARCH_INDEX_PREFIX
    research_cursor_ttl_seconds: int = 900
    request_timeout_seconds: float = 5.0
    environment: str = "production"
    auth_disabled: bool = False
    oidc_issuer: str | None = None
    oidc_jwks_uri: str | None = None
    oidc_audience: str | None = None
    oidc_required_scope: str = REQUIRED_SCOPE
    oidc_owner_subject: str | None = None
    allow_insecure_opensearch: bool = False

    def __post_init__(self) -> None:
        if self.environment not in {"production", "development", "test"}:
            raise ValueError("environment must be production, development, or test")
        if self.auth_disabled and self.environment != "test":
            raise ValueError("OIDC can only be disabled in the test environment")
        if self.allow_insecure_opensearch and self.environment == "production":
            raise ValueError("production OpenSearch endpoint must use https")
        if len(self.cursor_secret.encode("utf-8")) < 32:
            raise ValueError("cursor signing secret must contain at least 32 bytes")
        if re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,254}", self.read_alias) is None:
            raise ValueError("read alias is not a safe OpenSearch name")
        if (
            re.fullmatch(
                r"[a-z0-9][a-z0-9_-]{0,254}",
                self.research_read_alias,
            )
            is None
        ):
            raise ValueError("research read alias is not a safe OpenSearch name")
        if (
            re.fullmatch(
                r"[a-z0-9][a-z0-9_-]{0,254}",
                self.research_index_prefix,
            )
            is None
        ):
            raise ValueError("research index prefix is not a safe OpenSearch name")
        if (
            self.research_read_alias != RESEARCH_READ_ALIAS
            or self.research_index_prefix != RESEARCH_INDEX_PREFIX
        ):
            raise ValueError("research index and alias names are fixed")
        if not 1 <= self.research_cursor_ttl_seconds <= 3600:
            raise ValueError("research cursor TTL must be between 1 and 3600")
        OpenSearchConnection(
            endpoint=self.opensearch_endpoint,
            aws_region=self.aws_region,
            service=self.opensearch_service,
            timeout_seconds=self.request_timeout_seconds,
            allow_insecure=self.allow_insecure_opensearch,
        )
        if not self.auth_disabled and not all(
            (
                self.oidc_issuer,
                self.oidc_jwks_uri,
                self.oidc_audience,
                self.oidc_owner_subject,
            )
        ):
            raise ValueError(
                "OIDC issuer, JWKS URI, audience, and owner subject are required "
                "when auth is enabled"
            )
        if not self.auth_disabled:
            assert self.oidc_owner_subject is not None
            require_oidc_subject(self.oidc_owner_subject)
            self.oidc_config()

    @classmethod
    def from_env(cls) -> APISettings:
        return cls(
            opensearch_endpoint=_environment_value(
                "OPENSEARCH_ENDPOINT",
                "MEDIA_CATALOG_OPENSEARCH_ENDPOINT",
            ),
            cursor_secret=_environment_value(
                "CURSOR_SECRET",
                "MEDIA_CATALOG_CURSOR_SECRET",
            ),
            aws_region=_environment_value("REGION", "AWS_REGION") or None,
            opensearch_service=_environment_value(
                "MEDIA_CATALOG_OPENSEARCH_SERVICE",
                default="es",
            ),
            read_alias=_environment_value(
                "INDEX_ALIAS",
                "MEDIA_CATALOG_READ_ALIAS",
                default=READ_ALIAS,
            ),
            research_read_alias=_environment_value(
                "MEDIA_CATALOG_RESEARCH_READ_ALIAS",
                default=RESEARCH_READ_ALIAS,
            ),
            research_index_prefix=_environment_value(
                "MEDIA_CATALOG_RESEARCH_INDEX_PREFIX",
                default=RESEARCH_INDEX_PREFIX,
            ),
            research_cursor_ttl_seconds=int(
                os.environ.get(
                    "MEDIA_CATALOG_RESEARCH_CURSOR_TTL_SECONDS",
                    "900",
                )
            ),
            request_timeout_seconds=float(
                os.environ.get("MEDIA_CATALOG_SEARCH_TIMEOUT_SECONDS", "5")
            ),
            environment=os.environ.get(
                "MEDIA_CATALOG_ENVIRONMENT", "production"
            ).lower(),
            auth_disabled=_environment_bool("MEDIA_CATALOG_AUTH_DISABLED"),
            oidc_issuer=_environment_value(
                "OIDC_ISSUER",
                "MEDIA_CATALOG_OIDC_ISSUER",
            )
            or None,
            oidc_jwks_uri=_environment_value(
                "OIDC_JWKS_URI",
                "MEDIA_CATALOG_OIDC_JWKS_URI",
            )
            or None,
            oidc_audience=_environment_value(
                "OIDC_AUDIENCE",
                "MEDIA_CATALOG_OIDC_AUDIENCE",
            )
            or None,
            oidc_required_scope=_environment_value(
                "OIDC_REQUIRED_SCOPE",
                "MEDIA_CATALOG_OIDC_REQUIRED_SCOPE",
                default=REQUIRED_SCOPE,
            ),
            oidc_owner_subject=_environment_value(
                "OIDC_OWNER_SUBJECT",
                "MEDIA_CATALOG_OIDC_OWNER_SUBJECT",
            )
            or None,
            allow_insecure_opensearch=_environment_bool(
                "MEDIA_CATALOG_ALLOW_INSECURE_OPENSEARCH"
            ),
        )

    def opensearch_connection(self) -> OpenSearchConnection:
        return OpenSearchConnection(
            endpoint=self.opensearch_endpoint,
            aws_region=self.aws_region,
            service=self.opensearch_service,
            timeout_seconds=self.request_timeout_seconds,
            allow_insecure=self.allow_insecure_opensearch,
        )

    def oidc_config(self) -> OIDCConfig:
        if not all((self.oidc_issuer, self.oidc_jwks_uri, self.oidc_audience)):
            raise ValueError("OIDC configuration is incomplete")
        return OIDCConfig(
            issuer=self.oidc_issuer,
            jwks_uri=self.oidc_jwks_uri,
            audience=self.oidc_audience,
            required_scope=self.oidc_required_scope,
        )


class EntityType(StrEnum):
    MOVIE = "MOVIE"
    TV_SERIES = "TV_SERIES"
    TV_SEASON = "TV_SEASON"
    TV_EPISODE = "TV_EPISODE"
    PERSON = "PERSON"
    ORGANIZATION = "ORGANIZATION"
    UNKNOWN = "UNKNOWN"


@dataclass
class UpstreamFailure(Exception):
    status: int
    title: str
    detail: str


def _problem(
    request: Request,
    *,
    status: int,
    title: str,
    detail: str,
    code: str | None = None,
    retryable: bool | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    default_code, default_retryable = _PROBLEM_DEFAULTS.get(
        status,
        ("REQUEST_FAILED", False),
    )
    return JSONResponse(
        status_code=status,
        media_type="application/problem+json",
        headers=headers,
        content={
            "type": f"https://video-governance.example/problems/{status}",
            "title": title,
            "status": status,
            "code": code or default_code,
            "detail": detail,
            "retryable": (default_retryable if retryable is None else retryable),
            "instance": request.url.path,
        },
    )


def _total(value: Any) -> dict[str, Any]:
    if isinstance(value, int) and value >= 0:
        return {"value": value, "relation": "eq"}
    if not isinstance(value, dict):
        raise UpstreamFailure(502, "Bad Gateway", "Search response is invalid")
    count = value.get("value")
    relation = value.get("relation")
    if not isinstance(count, int) or count < 0 or relation not in {"eq", "gte"}:
        raise UpstreamFailure(502, "Bad Gateway", "Search total is invalid")
    return {"value": count, "relation": relation}


def _hits(response: Any) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not isinstance(response, dict) or response.get("timed_out") is True:
        if isinstance(response, dict) and response.get("timed_out") is True:
            raise UpstreamFailure(504, "Gateway Timeout", "Search timed out")
        raise UpstreamFailure(502, "Bad Gateway", "Search response is invalid")
    hits_block = response.get("hits")
    if not isinstance(hits_block, dict) or not isinstance(hits_block.get("hits"), list):
        raise UpstreamFailure(502, "Bad Gateway", "Search response is invalid")
    return hits_block["hits"], _total(hits_block.get("total"))


def _source(hit: Any) -> dict[str, Any]:
    if not isinstance(hit, dict) or not isinstance(hit.get("_source"), dict):
        raise UpstreamFailure(502, "Bad Gateway", "Search hit is invalid")
    return hit["_source"]


def _search_get(
    client: Any,
    *,
    alias: str,
    body: dict[str, Any],
    timeout_seconds: float,
) -> Any:
    path = f"/{quote(alias, safe='')}/_search"
    return client.transport.perform_request(
        method="GET",
        url=path,
        body=body,
        timeout=timeout_seconds,
    )


def _call_opensearch(
    operation: Callable[[], Any],
    *,
    allow_not_found: bool = False,
) -> Any:
    try:
        return operation()
    except UpstreamFailure:
        raise
    except Exception as exc:
        status = getattr(exc, "status_code", None)
        if status == 404 and allow_not_found:
            raise
        if isinstance(exc, TimeoutError) or "timeout" in type(exc).__name__.lower():
            raise UpstreamFailure(
                504,
                "Gateway Timeout",
                "OpenSearch request timed out",
            ) from exc
        raise UpstreamFailure(
            502,
            "Bad Gateway",
            "OpenSearch request failed",
        ) from exc


def _normalize_language(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.lower().replace("_", "-")
    if _LANGUAGE.fullmatch(normalized) is None:
        raise HTTPException(status_code=422, detail="language is invalid")
    return normalized


def _validate_text(value: str, *, label: str) -> str:
    normalized = value.strip()
    if not normalized or _CONTROL_CHARACTER.search(value):
        raise HTTPException(status_code=422, detail=f"{label} is invalid")
    return normalized


def _normalize_search_text(value: str) -> str:
    if _CONTROL_CHARACTER.search(value):
        raise HTTPException(status_code=422, detail="q is invalid")
    return value.strip()


def _entity_with_description(
    source: dict[str, Any],
    *,
    requested_language: str | None,
) -> dict[str, Any]:
    value = dict(source)
    descriptions = value.get("descriptions")
    value["description"] = select_description(
        descriptions if isinstance(descriptions, list) else [],
        requested_language=requested_language,
        display_language=(
            value.get("displayLanguage")
            if isinstance(value.get("displayLanguage"), str)
            else None
        ),
    )
    return value


def _summary(
    hit: Any,
    *,
    requested_language: str | None,
) -> dict[str, Any]:
    source = _source(hit)
    identifiers = source.get("externalIdentifiers")
    description = select_description(
        source.get("descriptions")
        if isinstance(source.get("descriptions"), list)
        else [],
        requested_language=requested_language,
        display_language=(
            source.get("displayLanguage")
            if isinstance(source.get("displayLanguage"), str)
            else None
        ),
    )
    return {
        "entityKey": source.get("entityKey"),
        "entityType": source.get("entityType"),
        "displayName": source.get("displayName"),
        "displayLanguage": source.get("displayLanguage"),
        "description": description,
        "externalIdentifiers": (
            identifiers[:_SUMMARY_IDENTIFIER_LIMIT]
            if isinstance(identifiers, list)
            else []
        ),
    }


def _gold_summary(hit: Any) -> dict[str, Any]:
    source = _source(hit)
    identifiers = source.get("externalIdentifiers")
    source_badges = source.get("sourceBadges")
    return {
        "entityKey": source.get("entityKey"),
        "entityLevel": source.get("entityLevel"),
        "entityKind": source.get("entityKind"),
        "displayName": source.get("displayName"),
        "displayLanguage": source.get("displayLanguage"),
        "releasePlanId": source.get("releasePlanId"),
        "contextId": source.get("contextId"),
        "conflictCount": source.get("conflictCount", 0),
        "externalIdentifiers": (
            identifiers[:_SUMMARY_IDENTIFIER_LIMIT]
            if isinstance(identifiers, list)
            else []
        ),
        "sourceBadges": (
            source_badges[:_SUMMARY_SOURCE_BADGE_LIMIT]
            if isinstance(source_badges, list)
            else []
        ),
    }


def _validate_query_parameters(request: Request, allowed: set[str]) -> None:
    names = [name for name, _ in request.query_params.multi_items()]
    if set(names) - allowed or len(names) != len(set(names)):
        raise HTTPException(status_code=422, detail="query parameters are invalid")


def create_app(
    settings: APISettings | None = None,
    *,
    client: Any | None = None,
    verifier: TokenVerifier | None = None,
) -> FastAPI:
    """Create the API; production configuration is validated before startup."""

    settings = settings or APISettings.from_env()
    if not settings.auth_disabled:
        verifier = verifier or OIDCJWTVerifier(settings.oidc_config())
    owns_client = client is None
    client = client or create_opensearch_client(settings.opensearch_connection())
    cursor_codec = CursorCodec(settings.cursor_secret.encode("utf-8"))
    research_cursor_codec = GoldCursorCodec(
        settings.cursor_secret.encode("utf-8"),
        ttl_seconds=settings.research_cursor_ttl_seconds,
        index_prefix=settings.research_index_prefix,
    )
    timeout_ms = int(settings.request_timeout_seconds * 1000)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        yield
        if owns_client:
            transport = getattr(client, "transport", None)
            if transport is not None and hasattr(transport, "close"):
                transport.close()

    app = FastAPI(
        title="Video Media Catalog API",
        version="1.0.0",
        description=(
            "Read-only API over the rebuildable OpenSearch projection. "
            "Iceberg remains the source of truth."
        ),
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.exception_handler(AuthenticationError)
    async def authentication_problem(
        request: Request, _: AuthenticationError
    ) -> JSONResponse:
        return _problem(
            request,
            status=401,
            title="Unauthorized",
            detail="A valid bearer token is required",
            code="AUTHENTICATION_REQUIRED",
            retryable=False,
            headers={"WWW-Authenticate": "Bearer"},
        )

    @app.exception_handler(OwnerAuthorizationError)
    async def owner_authorization_problem(
        request: Request, _: OwnerAuthorizationError
    ) -> JSONResponse:
        return _problem(
            request,
            status=403,
            title="Forbidden",
            detail="The personal research catalog is owner-only",
            code="OWNER_ONLY",
            retryable=False,
        )

    @app.exception_handler(ResearchScopeAuthorizationError)
    async def research_scope_problem(
        request: Request, _: ResearchScopeAuthorizationError
    ) -> JSONResponse:
        return _problem(
            request,
            status=403,
            title="Forbidden",
            detail=f"The {REQUIRED_SCOPE} scope is required",
            code="INSUFFICIENT_SCOPE",
            retryable=False,
        )

    @app.exception_handler(AuthorizationError)
    async def authorization_problem(
        request: Request, _: AuthorizationError
    ) -> JSONResponse:
        return _problem(
            request,
            status=403,
            title="Forbidden",
            detail=f"The {settings.oidc_required_scope} scope is required",
            code="INSUFFICIENT_SCOPE",
            retryable=False,
        )

    @app.exception_handler(InvalidCursor)
    async def cursor_problem(request: Request, _: InvalidCursor) -> JSONResponse:
        return _problem(
            request,
            status=400,
            title="Bad Request",
            detail="The search cursor is invalid",
            code="INVALID_CURSOR",
            retryable=False,
        )

    @app.exception_handler(UpstreamFailure)
    async def upstream_problem(request: Request, exc: UpstreamFailure) -> JSONResponse:
        return _problem(
            request,
            status=exc.status,
            title=exc.title,
            detail=exc.detail,
        )

    @app.exception_handler(RequestValidationError)
    async def validation_problem(
        request: Request, _: RequestValidationError
    ) -> JSONResponse:
        return _problem(
            request,
            status=422,
            title="Unprocessable Content",
            detail="Request parameters are invalid",
        )

    @app.exception_handler(ResponseValidationError)
    async def response_validation_problem(
        request: Request, _: ResponseValidationError
    ) -> JSONResponse:
        return _problem(
            request,
            status=502,
            title="Bad Gateway",
            detail="OpenSearch response does not match the API contract",
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_problem(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        return _problem(
            request,
            status=exc.status_code,
            title="Request Failed",
            detail=str(exc.detail),
            headers=exc.headers,
        )

    @app.exception_handler(Exception)
    async def internal_problem(request: Request, _: Exception) -> JSONResponse:
        return _problem(
            request,
            status=500,
            title="Internal Server Error",
            detail="The request could not be completed",
        )

    def require_principal(
        credentials: HTTPAuthorizationCredentials | None = _BEARER_DEPENDENCY,
    ) -> Principal:
        if settings.auth_disabled:
            return Principal(
                subject=settings.oidc_owner_subject or "test-owner",
                scopes=frozenset({REQUIRED_SCOPE}),
            )
        if (
            credentials is None
            or credentials.scheme.lower() != "bearer"
            or not credentials.credentials
        ):
            raise AuthenticationError("bearer token is required")
        assert verifier is not None
        return verifier.verify(credentials.credentials)

    def require_research_principal(
        principal=Depends(require_principal),  # noqa: B008
    ) -> Principal:
        return authorize_research_principal(
            principal,
            owner_subject=settings.oidc_owner_subject or "test-owner",
        )

    @app.get(
        "/openapi.json",
        include_in_schema=False,
        dependencies=[Depends(require_principal)],
    )
    def openapi_document() -> JSONResponse:
        return JSONResponse(app.openapi())

    @app.get(
        "/healthz",
        tags=["health"],
        summary="Liveness probe",
        response_model=HealthResponse,
        responses={500: {"model": ProblemDetails}},
    )
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get(
        "/api/v1/catalog/search",
        tags=["catalog"],
        summary="Search catalog entities",
        dependencies=[Depends(require_principal)],
        response_model=SearchResponse,
        responses=_AUTHENTICATED_ERRORS,
    )
    def search_catalog(
        request: Request,
        q: Annotated[str, Query(max_length=200)] = "",
        entity_type: Annotated[
            EntityType | None,
            Query(alias="entityType"),
        ] = None,
        language: Annotated[
            str | None,
            Query(min_length=2, max_length=35),
        ] = None,
        page_size: Annotated[
            int,
            Query(alias="pageSize", ge=1, le=100),
        ] = 20,
        cursor: Annotated[
            str | None,
            Query(min_length=10, max_length=4096),
        ] = None,
    ) -> dict[str, Any]:
        _validate_query_parameters(
            request,
            {"q", "entityType", "language", "pageSize", "cursor"},
        )
        parameters = SearchParameters(
            q=_normalize_search_text(q),
            entity_type=entity_type.value if entity_type is not None else None,
            language=_normalize_language(language),
            page_size=page_size,
        )
        search_after = (
            cursor_codec.decode(cursor, fingerprint=parameters.fingerprint())
            if cursor
            else None
        )
        response = _call_opensearch(
            lambda: _search_get(
                client,
                alias=settings.read_alias,
                body=build_search_query(
                    parameters,
                    search_after=search_after,
                    timeout_ms=timeout_ms,
                ),
                timeout_seconds=settings.request_timeout_seconds,
            )
        )
        hits, total = _hits(response)
        items = [_summary(hit, requested_language=parameters.language) for hit in hits]
        next_cursor = None
        if len(hits) == page_size:
            sort = hits[-1].get("sort") if isinstance(hits[-1], dict) else None
            if not isinstance(sort, list):
                raise UpstreamFailure(
                    502,
                    "Bad Gateway",
                    "Search hit has no stable sort tuple",
                )
            next_cursor = cursor_codec.encode(
                sort,
                fingerprint=parameters.fingerprint(),
            )
        return {
            "items": items,
            "nextCursor": next_cursor,
            "totalValue": total["value"],
            "totalRelation": total["relation"],
        }

    @app.get(
        "/api/v1/catalog/entities/{entityKey}",
        tags=["catalog"],
        summary="Get one catalog entity",
        dependencies=[Depends(require_principal)],
        response_model=CatalogEntity,
        response_model_exclude_none=True,
        responses={
            **_AUTHENTICATED_ERRORS,
            404: {"model": ProblemDetails},
        },
    )
    def get_entity(
        request: Request,
        entity_key: Annotated[
            str,
            Path(alias="entityKey", pattern=_ENTITY_KEY),
        ],
    ) -> dict[str, Any]:
        _validate_query_parameters(request, set())
        try:
            response = _call_opensearch(
                lambda: client.get(
                    index=settings.read_alias,
                    id=entity_key,
                    request_timeout=settings.request_timeout_seconds,
                ),
                allow_not_found=True,
            )
        except Exception as exc:
            if getattr(exc, "status_code", None) == 404:
                raise HTTPException(
                    status_code=404,
                    detail="Catalog entity was not found",
                ) from exc
            raise
        if not isinstance(response, dict) or not isinstance(
            response.get("_source"), dict
        ):
            raise UpstreamFailure(
                502,
                "Bad Gateway",
                "Entity response is invalid",
            )
        return _entity_with_description(
            response["_source"],
            requested_language=None,
        )

    @app.get(
        "/api/v1/catalog/external-identifiers/{scheme}/{value:path}",
        tags=["catalog"],
        summary="Resolve an external identifier",
        dependencies=[Depends(require_principal)],
        response_model=CatalogEntity,
        response_model_exclude_none=True,
        responses={
            **_AUTHENTICATED_ERRORS,
            404: {"model": ProblemDetails},
            409: {"model": ProblemDetails},
        },
    )
    def resolve_external_identifier(
        request: Request,
        scheme: Annotated[str, Path(pattern=_SCHEME)],
        value: Annotated[str, Path(min_length=1, max_length=256)],
    ) -> dict[str, Any]:
        _validate_query_parameters(request, set())
        identifier = _validate_text(value, label="external identifier")
        response = _call_opensearch(
            lambda: _search_get(
                client,
                alias=settings.read_alias,
                body=build_external_identifier_query(
                    scheme=scheme,
                    value=identifier,
                    timeout_ms=timeout_ms,
                ),
                timeout_seconds=settings.request_timeout_seconds,
            )
        )
        hits, total = _hits(response)
        if total["value"] == 0 or not hits:
            raise HTTPException(
                status_code=404,
                detail="External identifier was not found",
            )
        if total["relation"] != "eq" or total["value"] != 1 or len(hits) != 1:
            raise HTTPException(
                status_code=409,
                detail="External identifier resolves to multiple entities",
            )
        return _entity_with_description(
            _source(hits[0]),
            requested_language=None,
        )

    @app.get(
        "/api/v2/research/search",
        tags=["personal-research-v2"],
        summary="Search the owner-only personal research catalog",
        dependencies=[Depends(require_research_principal)],
        response_model=GoldSearchResponse,
        responses=_AUTHENTICATED_ERRORS,
    )
    def search_gold_catalog(
        request: Request,
        q: Annotated[str, Query(max_length=200)] = "",
        entity_level: Annotated[
            str | None,
            Query(alias="entityLevel", pattern=_ENTITY_CLASS),
        ] = None,
        entity_kind: Annotated[
            str | None,
            Query(alias="entityKind", pattern=_ENTITY_CLASS),
        ] = None,
        language: Annotated[
            str | None,
            Query(min_length=2, max_length=35),
        ] = None,
        has_conflicts: Annotated[
            bool | None,
            Query(alias="hasConflicts"),
        ] = None,
        page_size: Annotated[
            int,
            Query(alias="pageSize", ge=1, le=100),
        ] = 20,
        cursor: Annotated[
            str | None,
            Query(min_length=10, max_length=4096),
        ] = None,
    ) -> dict[str, Any]:
        _validate_query_parameters(
            request,
            {
                "q",
                "entityLevel",
                "entityKind",
                "language",
                "hasConflicts",
                "pageSize",
                "cursor",
            },
        )
        parameters = GoldSearchParameters(
            q=_normalize_search_text(q),
            entity_level=entity_level,
            entity_kind=entity_kind,
            language=_normalize_language(language),
            has_conflicts=has_conflicts,
            page_size=page_size,
        )
        state = (
            research_cursor_codec.decode(
                cursor,
                fingerprint=parameters.fingerprint(),
            )
            if cursor
            else None
        )
        response = _call_opensearch(
            lambda: _search_get(
                client,
                alias=(
                    state.index if state is not None else settings.research_read_alias
                ),
                body=build_gold_search_query(
                    parameters,
                    search_after=(state.sort if state is not None else None),
                    timeout_ms=timeout_ms,
                ),
                timeout_seconds=settings.request_timeout_seconds,
            )
        )
        hits, total = _hits(response)
        next_cursor = None
        if len(hits) == page_size:
            indexes = {hit.get("_index") for hit in hits if isinstance(hit, dict)}
            sort = hits[-1].get("sort") if isinstance(hits[-1], dict) else None
            if (
                len(indexes) != 1
                or not isinstance(next(iter(indexes)), str)
                or not isinstance(sort, list)
            ):
                raise UpstreamFailure(
                    502,
                    "Bad Gateway",
                    "Gold search hit has no stable index and sort tuple",
                )
            next_cursor = research_cursor_codec.encode(
                index=next(iter(indexes)),
                sort=sort,
                fingerprint=parameters.fingerprint(),
            )
        return {
            "items": [_gold_summary(hit) for hit in hits],
            "nextCursor": next_cursor,
            "totalValue": total["value"],
            "totalRelation": total["relation"],
        }

    @app.get(
        "/api/v2/research/entities/{entityKey}",
        tags=["personal-research-v2"],
        summary="Get one owner-only personal research entity",
        dependencies=[Depends(require_research_principal)],
        response_model=GoldCatalogEntity,
        responses={
            **_AUTHENTICATED_ERRORS,
            404: {"model": ProblemDetails},
        },
    )
    def get_gold_entity(
        request: Request,
        entity_key: Annotated[
            str,
            Path(alias="entityKey", pattern=_ENTITY_KEY),
        ],
    ) -> dict[str, Any]:
        _validate_query_parameters(request, set())
        try:
            response = _call_opensearch(
                lambda: client.get(
                    index=settings.research_read_alias,
                    id=entity_key,
                    request_timeout=settings.request_timeout_seconds,
                ),
                allow_not_found=True,
            )
        except Exception as exc:
            if getattr(exc, "status_code", None) == 404:
                raise HTTPException(
                    status_code=404,
                    detail="Research catalog entity was not found",
                ) from exc
            raise
        if not isinstance(response, dict) or not isinstance(
            response.get("_source"), dict
        ):
            raise UpstreamFailure(
                502,
                "Bad Gateway",
                "Research entity response is invalid",
            )
        return response["_source"]

    @app.get(
        "/api/v2/research/external-identifiers/{namespace}/{value:path}",
        tags=["personal-research-v2"],
        summary="Resolve one personal research external identifier",
        dependencies=[Depends(require_research_principal)],
        response_model=GoldCatalogEntity,
        responses={
            **_AUTHENTICATED_ERRORS,
            404: {"model": ProblemDetails},
            409: {"model": ProblemDetails},
        },
    )
    def resolve_gold_external_identifier(
        request: Request,
        namespace: Annotated[str, Path(pattern=_SCHEME)],
        value: Annotated[str, Path(min_length=1, max_length=256)],
    ) -> dict[str, Any]:
        _validate_query_parameters(request, set())
        identifier = _validate_text(value, label="external identifier")
        response = _call_opensearch(
            lambda: _search_get(
                client,
                alias=settings.research_read_alias,
                body=build_gold_external_identifier_query(
                    namespace=namespace,
                    value=identifier,
                    timeout_ms=timeout_ms,
                ),
                timeout_seconds=settings.request_timeout_seconds,
            )
        )
        hits, total = _hits(response)
        if total["value"] == 0 or not hits:
            raise HTTPException(
                status_code=404,
                detail="Research external identifier was not found",
            )
        if total["relation"] != "eq" or total["value"] != 1 or len(hits) != 1:
            raise HTTPException(
                status_code=409,
                detail="Research external identifier resolves to multiple entities",
            )
        return _source(hits[0])

    return app
