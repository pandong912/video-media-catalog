"""Shared Spark configuration helpers for batch data processing."""

from __future__ import annotations

from typing import Any


def spark_uri(uri: str) -> str:
    """Translate S3 object URIs for Hadoop S3A readers."""

    return "s3a://" + uri[len("s3://") :] if uri.startswith("s3://") else uri


def configure_s3a_builder(
    builder: Any,
    *,
    aws_region: str | None,
    s3_endpoint: str | None,
    s3_path_style_access: bool,
    credentials_provider: str = "web-identity",
) -> Any:
    providers = {
        "default": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        "web-identity": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
    }
    try:
        provider = providers[credentials_provider]
    except KeyError as exc:
        raise ValueError(
            "credentials_provider must be 'default' or 'web-identity'"
        ) from exc
    builder = builder.config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        provider,
    )
    if aws_region:
        builder = builder.config(
            "spark.hadoop.fs.s3a.endpoint.region",
            aws_region,
        )
    if s3_endpoint:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
    if s3_path_style_access:
        builder = builder.config("spark.hadoop.fs.s3a.path.style.access", "true")
    return builder
