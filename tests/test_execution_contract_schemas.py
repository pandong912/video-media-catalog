from __future__ import annotations

import hashlib
import json
import tomllib
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

ROOT = Path(__file__).resolve().parents[1]
CONTRACTS = ROOT / "contracts/execution"
LOCK = CONTRACTS / "platform-execution-contracts.v1.lock.json"

SCHEMA_FIXTURES = {
    "execution-input-manifest.v1.schema.json": "execution-input-manifest.json",
    "execution-plan-manifest.v1.schema.json": "execution-plan-manifest.json",
    "execution-plan-request.v1.schema.json": "execution-plan-request.json",
    "object-ref-v2.v2.schema.json": "object-ref-v2.json",
    "partition-receipt.v1.schema.json": "partition-receipt.json",
    "receipt-set.v1.schema.json": "receipt-set.json",
    "root-manifest.v1.schema.json": "root-manifest.json",
}


def _json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def _registry() -> Registry:
    registry = Registry()
    for schema_name in SCHEMA_FIXTURES:
        document = _json(CONTRACTS / schema_name)
        registry = registry.with_resource(
            schema_name,
            Resource.from_contents(document),
        )
    return registry


@pytest.mark.parametrize(("schema_name", "fixture_name"), SCHEMA_FIXTURES.items())
def test_vendored_schema_accepts_valid_and_rejects_invalid_fixture(
    schema_name: str,
    fixture_name: str,
) -> None:
    schema = _json(CONTRACTS / schema_name)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(
        schema,
        registry=_registry(),
        format_checker=FormatChecker(),
    )

    valid = _json(CONTRACTS / "fixtures/valid" / fixture_name)
    invalid = _json(CONTRACTS / "fixtures/invalid" / fixture_name)

    assert list(validator.iter_errors(valid)) == []
    assert list(validator.iter_errors(invalid))


def test_vendored_bytes_match_platform_source_lock() -> None:
    lock = _json(LOCK)
    assert lock["sourceRepository"] == "video-world-platform"
    assert lock["sourceCommit"] == "65a4234710d3e151e773a503938b8663af5d82ad"
    assert lock["sourcePath"] == "contracts/execution"
    assert lock["sourceContractReadmeSha256"] == (
        "cffa32baae0c9f9d78fa36b29d58d791847b1f90b729153bd0e554e6e0c68201"
    )
    assert lock["sourceFixtureGeneratorSha256"] == (
        "3b043fc50ee917cd2daea56e605fa78c9ea6daf1bb9a0de9d615e41871f5f966"
    )
    assert lock["sourceCanonicalDigestImplementationSha256"] == (
        "bbe5d2d1e9f3f98286406814c3ccf0bfeb59f54016974c4263fc43e165eeec32"
    )

    locked_paths: set[str] = set()
    for entry in lock["files"]:
        relative = entry["path"]
        locked_paths.add(relative)
        assert (
            hashlib.sha256((CONTRACTS / relative).read_bytes()).hexdigest()
            == (entry["sha256"])
        )

    vendored_paths = {
        path.relative_to(CONTRACTS).as_posix()
        for pattern in (
            "*.schema.json",
            "*.golden.json",
            "fixtures/valid/*.json",
            "fixtures/invalid/*.json",
        )
        for path in CONTRACTS.glob(pattern)
    }
    assert locked_paths == vendored_paths


def test_only_remaining_upstream_issue_is_untracked_source_provenance() -> None:
    lock = _json(LOCK)
    issue_ids = {issue["id"] for issue in lock["provenanceLimitations"]}
    assert issue_ids == {"SOURCE_CONTRACTS_NOT_IN_SOURCE_COMMIT"}


def test_object_ref_schema_enforces_provider_and_immutable_coordinates() -> None:
    schema = _json(CONTRACTS / "object-ref-v2.v2.schema.json")
    validator = Draft202012Validator(
        schema,
        registry=_registry(),
        format_checker=FormatChecker(),
    )
    fixture = _json(CONTRACTS / "fixtures/valid/object-ref-v2.json")

    assert list(validator.iter_errors(fixture)) == []
    for field, value in (
        ("uri", f"{fixture['uri']}?X-Amz-Credential=secret"),
        ("versionToken", "latest"),
        ("provider", "UNKNOWN"),
    ):
        invalid = dict(fixture)
        invalid[field] = value
        assert list(validator.iter_errors(invalid))

    root_schema = _json(CONTRACTS / "root-manifest.v1.schema.json")
    root_fields = root_schema["properties"]
    assert {
        "planManifestObject",
        "planManifestSemanticDigest",
        "receiptSetObject",
        "receiptSetSemanticDigest",
        "resultKind",
        "resultDigest",
        "resultObjects",
    } <= set(root_fields)


def test_wheel_force_includes_the_complete_execution_contract_tree() -> None:
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text())
    force_include = pyproject["tool"]["hatch"]["build"]["targets"]["wheel"][
        "force-include"
    ]
    assert force_include["contracts/execution"] == (
        "video_media_catalog/contracts/execution"
    )

    projected_members = {
        (
            Path(force_include["contracts/execution"]) / path.relative_to(CONTRACTS)
        ).as_posix()
        for path in CONTRACTS.rglob("*")
        if path.is_file()
    }
    assert (
        "video_media_catalog/contracts/execution/canonical-tuple-v1.golden.json"
        in projected_members
    )
    assert "video_media_catalog/contracts/execution/README.md" in projected_members
    assert (
        "video_media_catalog/contracts/execution/"
        "platform-execution-contracts.v1.lock.json"
    ) in projected_members
    assert len(_json(LOCK)["files"]) == 22
    for schema_name, fixture_name in SCHEMA_FIXTURES.items():
        assert (
            f"video_media_catalog/contracts/execution/{schema_name}"
            in projected_members
        )
        assert (
            f"video_media_catalog/contracts/execution/fixtures/valid/{fixture_name}"
        ) in projected_members
        assert (
            f"video_media_catalog/contracts/execution/fixtures/invalid/{fixture_name}"
        ) in projected_members
