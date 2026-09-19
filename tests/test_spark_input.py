from __future__ import annotations

import pytest

from video_media_catalog.spark_input import configure_s3a_builder


class _Builder:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def config(self, key: str, value: str):
        self.values[key] = value
        return self


@pytest.mark.parametrize(
    ("name", "class_name"),
    [
        ("default", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"),
        ("web-identity", "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"),
    ],
)
def test_configure_s3a_builder_selects_credential_chain(
    name: str,
    class_name: str,
) -> None:
    builder = _Builder()

    configured = configure_s3a_builder(
        builder,
        aws_region="us-east-1",
        s3_endpoint=None,
        s3_path_style_access=False,
        credentials_provider=name,
    )

    assert configured is builder
    assert builder.values["spark.hadoop.fs.s3a.aws.credentials.provider"] == class_name


def test_configure_s3a_builder_rejects_unknown_credential_chain() -> None:
    with pytest.raises(ValueError, match="credentials_provider"):
        configure_s3a_builder(
            _Builder(),
            aws_region=None,
            s3_endpoint=None,
            s3_path_style_access=False,
            credentials_provider="instance-profile",
        )
