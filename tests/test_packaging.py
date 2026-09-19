from __future__ import annotations

from pathlib import Path


def test_docker_and_python_spark_versions_are_aligned() -> None:
    root = Path(__file__).resolve().parents[1]
    pyproject = (root / "pyproject.toml").read_text()
    dockerfile = (root / "Dockerfile").read_text()
    api_dockerfile = (root / "Dockerfile.api").read_text()
    publish_workflow = (root / ".github/workflows/publish.yml").read_text()

    assert "FROM eclipse-temurin:17-jre-noble" in dockerfile
    assert "slim-bookworm" not in dockerfile
    assert "slim-trixie" not in dockerfile
    assert '"pyspark==3.5.5"' in pyproject
    assert "ARG SPARK_VERSION=3.5.5" in dockerfile
    assert "ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.780" in dockerfile
    assert (
        "ARG AWS_JAVA_SDK_BUNDLE_SHA1=308a3af95a47e0c4e1f8bd98a37657d4661ae45e"
    ) in dockerfile
    assert '"$SPARK_HOME/kubernetes/dockerfiles/spark/entrypoint.sh"' in dockerfile
    assert "JAVA_HOME=/opt/java/openjdk" in dockerfile
    assert 'ENTRYPOINT ["/opt/entrypoint.sh"]' in dockerfile
    assert "COPY stage.py /opt/video-media-catalog/stage.py" in dockerfile
    assert (
        "COPY validate_stage.py /opt/video-media-catalog/validate_stage.py"
        in dockerfile
    )
    assert "WORKDIR /opt/spark/work-dir" in dockerfile
    assert "--extra index" in dockerfile

    assert api_dockerfile.startswith("FROM ubuntu:noble")
    assert "apt-get upgrade --yes" in api_dockerfile
    assert "python3.12" in api_dockerfile
    assert "SPARK_HOME" not in api_dockerfile
    assert "JAVA_HOME" not in api_dockerfile
    assert "--extra api" in api_dockerfile
    assert "USER 10001:10001" in api_dockerfile
    assert "HEALTHCHECK" in api_dockerfile
    assert (
        'ENTRYPOINT ["/usr/bin/tini", "--", "video-media-catalog-api"]'
        in api_dockerfile
    )

    assert (
        'video-media-catalog-index = "video_media_catalog.index_cli:main"' in pyproject
    )
    assert 'video-media-catalog-api = "video_media_catalog.api_cli:main"' in pyproject
    assert (
        'video-media-catalog-validate = "video_media_catalog.validate_cli:main"'
        in pyproject
    )
    assert (
        "video-media-catalog-wikidata-sync = "
        '"video_media_catalog.wikidata_sync_cli:main"' in pyproject
    )
    assert (
        "video-media-catalog-wikidata-subset = "
        '"video_media_catalog.wikidata_subset_cli:main"' in pyproject
    )
    assert (
        "video-media-catalog-tvmaze-sync = "
        '"video_media_catalog.tvmaze_sync_cli:main"' in pyproject
    )
    assert "repository: video-media-catalog\n" in publish_workflow
    assert "repository: video-media-catalog-api" in publish_workflow
    assert "dockerfile: Dockerfile.api" in publish_workflow
    assert "findingSeverityCounts.CRITICAL" in publish_workflow
