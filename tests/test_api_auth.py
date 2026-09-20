from __future__ import annotations

import time
from types import SimpleNamespace

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from video_media_catalog.api_auth import (
    AuthenticationError,
    AuthorizationError,
    OIDCConfig,
    OIDCJWTVerifier,
    OwnerAuthorizationError,
    Principal,
    ResearchScopeAuthorizationError,
    authorize_research_principal,
)


class StaticJWKClient:
    def __init__(self, public_key) -> None:
        self.public_key = public_key

    def get_signing_key_from_jwt(self, _token: str):
        return SimpleNamespace(key=self.public_key)


@pytest.fixture
def oidc_keys():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return private_key, private_key.public_key()


def _token(private_key, **overrides):
    now = int(time.time())
    claims = {
        "iss": "https://issuer.example",
        "aud": "media-catalog-api",
        "sub": "user-123",
        "exp": now + 300,
        "iat": now,
        "scope": "profile governance.read",
        **overrides,
    }
    return jwt.encode(
        claims,
        private_key,
        algorithm="RS256",
        headers={"kid": "test-key"},
    )


def _verifier(public_key) -> OIDCJWTVerifier:
    return OIDCJWTVerifier(
        OIDCConfig(
            issuer="https://issuer.example",
            jwks_uri="https://issuer.example/.well-known/jwks.json",
            audience="media-catalog-api",
        ),
        jwk_client=StaticJWKClient(public_key),
    )


def test_oidc_allows_only_https_or_internal_cluster_jwks() -> None:
    config = OIDCConfig(
        issuer="https://issuer.example/oidc",
        jwks_uri="http://logto.logto.svc.cluster.local:3001/oidc/jwks",
        audience="media-catalog-api",
    )

    assert config.jwks_uri.startswith("http://")


@pytest.mark.parametrize(
    ("issuer", "jwks_uri"),
    [
        (
            "http://issuer.svc.cluster.local/oidc",
            "https://issuer.example/jwks",
        ),
        (
            "https://issuer.example/oidc?tenant=x",
            "https://issuer.example/jwks",
        ),
        (
            "https://issuer.example/oidc",
            "http://issuer.example/jwks",
        ),
        (
            "https://issuer.example/oidc",
            "http://evilsvc.cluster.local/jwks",
        ),
        (
            "https://issuer.example/oidc",
            "http://user:password@logto.svc.cluster.local/jwks",
        ),
        (
            "https://issuer.example/oidc",
            "http://logto.svc.cluster.local/jwks?x=1",
        ),
    ],
)
def test_oidc_rejects_unsafe_issuer_or_jwks(
    issuer: str,
    jwks_uri: str,
) -> None:
    with pytest.raises(ValueError, match="OIDC"):
        OIDCConfig(
            issuer=issuer,
            jwks_uri=jwks_uri,
            audience="media-catalog-api",
        )


def test_oidc_rejects_invalid_required_scope() -> None:
    with pytest.raises(ValueError, match="scope"):
        OIDCConfig(
            issuer="https://issuer.example",
            jwks_uri="https://issuer.example/jwks",
            audience="media-catalog-api",
            required_scope="governance.read another.scope",
        )


def test_oidc_accepts_valid_signed_token_with_required_scope(oidc_keys) -> None:
    private_key, public_key = oidc_keys

    principal = _verifier(public_key).verify(_token(private_key))

    assert principal.subject == "user-123"
    assert "governance.read" in principal.scopes


def test_oidc_rejects_wrong_audience(oidc_keys) -> None:
    private_key, public_key = oidc_keys

    with pytest.raises(AuthenticationError):
        _verifier(public_key).verify(_token(private_key, aud="another-service"))


def test_oidc_rejects_invalid_signature(oidc_keys) -> None:
    private_key, _ = oidc_keys
    another_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)

    with pytest.raises(AuthenticationError):
        _verifier(another_key.public_key()).verify(_token(private_key))


def test_oidc_rejects_missing_scope(oidc_keys) -> None:
    private_key, public_key = oidc_keys

    with pytest.raises(AuthorizationError):
        _verifier(public_key).verify(_token(private_key, scope="profile"))


def test_oidc_enforces_configured_required_scope(oidc_keys) -> None:
    private_key, public_key = oidc_keys
    verifier = OIDCJWTVerifier(
        OIDCConfig(
            issuer="https://issuer.example",
            jwks_uri="https://issuer.example/jwks",
            audience="media-catalog-api",
            required_scope="catalog.read",
        ),
        jwk_client=StaticJWKClient(public_key),
    )

    principal = verifier.verify(
        _token(private_key, scope="governance.read catalog.read")
    )

    assert "catalog.read" in principal.scopes


def test_research_authorization_requires_exact_owner_and_governance_scope() -> None:
    principal = Principal(
        subject="owner-123",
        scopes=frozenset({"governance.read"}),
    )
    assert (
        authorize_research_principal(
            principal,
            owner_subject="owner-123",
        )
        == principal
    )
    with pytest.raises(OwnerAuthorizationError):
        authorize_research_principal(
            principal,
            owner_subject="OWNER-123",
        )
    with pytest.raises(ResearchScopeAuthorizationError):
        authorize_research_principal(
            Principal(subject="owner-123", scopes=frozenset({"catalog.read"})),
            owner_subject="owner-123",
        )
