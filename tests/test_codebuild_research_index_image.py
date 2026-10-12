from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_research_index_image_is_built_remotely_from_versioned_source() -> None:
    contract = (ROOT / "buildspec.research-index-image.yml").read_text()

    assert (
        'test "${CODEBUILD_SOURCE_VERSION:?}" = "$SOURCE_OBJECT_VERSION"'
        in contract
    )
    assert 'test -z "${CODEBUILD_RESOLVED_SOURCE_VERSION:-}" || test' in contract
    assert "docker build \\" in contract
    assert "--platform linux/amd64" in contract
    assert "docker push" in contract
    assert 'echo "${COSIGN_SHA256}  /usr/local/bin/cosign"' in contract
    assert 'echo "${SYFT_SHA256}  /tmp/syft.tar.gz"' in contract
    assert "syft scan" in contract
    assert "sbom.spdx.json" in contract
    assert "cosign sign --yes --key" in contract
    assert "cosign verify --key" in contract
    assert "cosign attest --yes --key" in contract
    assert "cosign verify-attestation --key" in contract
    assert "criticalFindings" in contract
    assert "aws s3 cp /tmp/research-index-image-evidence.json" in contract
    assert "kubectl" not in contract
    assert "helm upgrade" not in contract
    assert "argocd" not in contract
