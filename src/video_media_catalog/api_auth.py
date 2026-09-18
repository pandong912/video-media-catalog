"""Fail-closed OIDC JWT verification for the catalog API."""

from __future__ import annotations

import re
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit

REQUIRED_SCOPE = "governance.read"
ALLOWED_JWT_ALGORITHMS = frozenset(
    {"RS256", "RS384", "RS512", "ES256", "ES384", "ES512"}
)
_SCOPE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def _validate_oidc_url(
    value: str,
    *,
    label: str,
    allow_internal_http: bool,
) -> None:
    parsed = urlsplit(value)
    try:
        _ = parsed.port
    except ValueError as exc:
        raise ValueError(f"OIDC {label} is not an allowed URL") from exc
    hostname = parsed.hostname
    internal_http = (
        parsed.scheme == "http"
        and hostname is not None
        and (hostname == "svc.cluster.local" or hostname.endswith(".svc.cluster.local"))
    )
    if (
        not value
        or any(character.isspace() for character in value)
        or hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or (parsed.scheme != "https" and not (allow_internal_http and internal_http))
    ):
        raise ValueError(f"OIDC {label} is not an allowed URL")


class AuthenticationError(Exception):
    """The bearer credential is absent or cannot be authenticated."""


class AuthorizationError(Exception):
    """The authenticated principal lacks required authorization."""


@dataclass(frozen=True)
class Principal:
    subject: str
    scopes: frozenset[str]


@dataclass(frozen=True)
class OIDCConfig:
    issuer: str
    jwks_uri: str
    audience: str
    required_scope: str = REQUIRED_SCOPE
    leeway_seconds: int = 30

    def __post_init__(self) -> None:
        _validate_oidc_url(
            self.issuer,
            label="issuer",
            allow_internal_http=False,
        )
        _validate_oidc_url(
            self.jwks_uri,
            label="JWKS URI",
            allow_internal_http=True,
        )
        if not self.audience.strip():
            raise ValueError("OIDC audience must not be empty")
        if _SCOPE.fullmatch(self.required_scope) is None:
            raise ValueError("required OIDC scope is invalid")
        if not 0 <= self.leeway_seconds <= 300:
            raise ValueError("OIDC leeway must be between 0 and 300 seconds")


class TokenVerifier(Protocol):
    def verify(self, token: str) -> Principal: ...


def _scopes(value: Any) -> frozenset[str]:
    if isinstance(value, str):
        return frozenset(item for item in value.split() if item)
    if isinstance(value, Collection) and not isinstance(
        value, (str, bytes, bytearray, dict)
    ):
        return frozenset(str(item) for item in value if isinstance(item, str))
    return frozenset()


class OIDCJWTVerifier:
    """Verify asymmetric OIDC JWTs against a cached remote JWKS."""

    def __init__(self, config: OIDCConfig, *, jwk_client: Any | None = None) -> None:
        import jwt

        self.config = config
        self.jwk_client = jwk_client or jwt.PyJWKClient(
            config.jwks_uri,
            cache_keys=True,
            cache_jwk_set=True,
            lifespan=300,
            timeout=5,
        )

    def verify(self, token: str) -> Principal:
        import jwt

        if not token or len(token) > 8192:
            raise AuthenticationError("invalid bearer token")
        try:
            header = jwt.get_unverified_header(token)
            algorithm = header.get("alg")
            if algorithm not in ALLOWED_JWT_ALGORITHMS:
                raise AuthenticationError("unsupported bearer token algorithm")
            signing_key = self.jwk_client.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=[algorithm],
                audience=self.config.audience,
                issuer=self.config.issuer,
                leeway=self.config.leeway_seconds,
                options={
                    "require": ["exp", "iss", "aud", "sub"],
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_iss": True,
                    "verify_aud": True,
                },
            )
        except (AuthenticationError, jwt.PyJWTError) as exc:
            raise AuthenticationError("bearer token verification failed") from exc
        except Exception as exc:
            raise AuthenticationError("OIDC key verification failed") from exc

        subject = claims.get("sub")
        if not isinstance(subject, str) or not subject.strip():
            raise AuthenticationError("bearer token subject is invalid")
        scopes = _scopes(claims.get("scope"))
        if self.config.required_scope not in scopes:
            raise AuthorizationError("required scope is missing")
        return Principal(subject=subject, scopes=scopes)
