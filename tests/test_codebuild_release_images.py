from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EMR_BASE_IMAGE = (
    "public.ecr.aws/emr-serverless/spark/emr-7.10.0@sha256:"
    "c612b1190b418da1163d1676c8c6903280d598e4c0d5ecccfbd8ebd8d228fd46"
)


def test_release_buildspec_signs_both_runtime_images_from_immutable_source() -> None:
    contract = (ROOT / "buildspec.release-images.yml").read_text()

    assert 'test "${CODEBUILD_SOURCE_VERSION:?}" = "$SOURCE_OBJECT_VERSION"' in contract
    assert "runtime|Dockerfile|release|10001:10001" in contract
    assert "emr|Dockerfile.emr|emr-release|hadoop:hadoop" in contract
    assert 'cosign sign --yes --key "awskms:///$KMS_KEY_ARN"' in contract
    assert contract.count("for attempt in $(seq 1 8)") == 2
    assert "cosign signing-config create" in contract
    assert "--signing-config /tmp/cosign-kms-only-signing-config.json" in contract
    assert "kms-only-rekor-unavailable" in contract
    assert "attestationTransparencyLog" in contract
    assert "cosign verify-attestation" in contract
    assert '"${GRYPE_VERSION:?}" "${GRYPE_SHA256:?}"' in contract
    assert '"${EMR_BASE_IMAGE:?}"' in contract
    assert EMR_BASE_IMAGE in contract
    assert "exceeds the maximum supported with Basic Scanning" in contract
    assert "scanner=grype-differential" in contract
    assert 'test "$new_critical" = 0' in contract
    assert "MANAGED_EMR_BASE_FINDINGS_RECORDED_NO_NEW_CRITICAL" in contract
    assert "MANAGED_EMR_BASE_FINDINGS_RECORDED" in contract
    assert "claimsLiveDeployment:false" in contract


def test_emr_image_pins_the_scanned_managed_base() -> None:
    dockerfile = (ROOT / "Dockerfile.emr").read_text()

    assert EMR_BASE_IMAGE in dockerfile
    assert "FROM ${EMR_BASE_IMAGE}" in dockerfile
