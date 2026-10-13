from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_release_buildspec_signs_both_runtime_images_from_immutable_source() -> None:
    contract = (ROOT / "buildspec.release-images.yml").read_text()

    assert 'test "${CODEBUILD_SOURCE_VERSION:?}" = "$SOURCE_OBJECT_VERSION"' in contract
    assert "runtime|Dockerfile|release|10001:10001" in contract
    assert "emr|Dockerfile.emr|emr-release|hadoop:hadoop" in contract
    assert 'cosign sign --yes --key "awskms:///$KMS_KEY_ARN"' in contract
    assert "cosign verify-attestation" in contract
    assert "criticalFindings:0" in contract
    assert "MANAGED_EMR_BASE_HIGH_FINDINGS_RECORDED" in contract
    assert "claimsLiveDeployment:false" in contract
