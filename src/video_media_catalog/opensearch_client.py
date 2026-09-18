"""OpenSearch clients authenticated with the AWS default credential chain."""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class OpenSearchConnection:
    endpoint: str
    aws_region: str | None = None
    service: str = "es"
    timeout_seconds: float = 10.0
    allow_insecure: bool = False

    def __post_init__(self) -> None:
        parsed = urlsplit(self.endpoint)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError("OpenSearch endpoint must be an origin URL")
        if parsed.scheme != "https" and not self.allow_insecure:
            raise ValueError("OpenSearch endpoint must use https")
        if self.service not in {"es", "aoss"}:
            raise ValueError("OpenSearch SigV4 service must be es or aoss")
        if not 0 < self.timeout_seconds <= 60:
            raise ValueError("OpenSearch timeout must be between 0 and 60 seconds")


def create_opensearch_client(config: OpenSearchConnection):
    """Create an opensearch-py client using refreshable AWS credentials."""

    import boto3
    from opensearchpy import (
        AWSV4SignerAuth,
        OpenSearch,
        RequestsHttpConnection,
    )

    parsed = urlsplit(config.endpoint)
    session = boto3.Session(region_name=config.aws_region)
    region = config.aws_region or session.region_name
    if not region:
        raise RuntimeError("AWS region is required for OpenSearch SigV4")
    credentials = session.get_credentials()
    if credentials is None:
        raise RuntimeError("AWS default credential chain returned no credentials")
    auth = AWSV4SignerAuth(credentials, region, config.service)
    use_ssl = parsed.scheme == "https"
    return OpenSearch(
        hosts=[
            {
                "host": parsed.hostname,
                "port": parsed.port or (443 if use_ssl else 80),
            }
        ],
        http_auth=auth,
        use_ssl=use_ssl,
        verify_certs=use_ssl,
        connection_class=RequestsHttpConnection,
        timeout=config.timeout_seconds,
        max_retries=3,
        retry_on_timeout=True,
        pool_maxsize=20,
    )
