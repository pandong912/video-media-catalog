from __future__ import annotations

from pathlib import Path


def test_batch_images_and_entrypoints_match_processing_scope() -> None:
    root = Path(__file__).resolve().parents[1]
    pyproject = (root / "pyproject.toml").read_text()
    dockerfile = (root / "Dockerfile").read_text()
    emr_dockerfile = (root / "Dockerfile.emr").read_text()
    publish_workflow = (root / ".github/workflows/publish.yml").read_text()
    ci_workflow = (root / ".github/workflows/ci.yml").read_text()
    api_dockerfile = "Dockerfile." + "api"
    api_entrypoint = "video-media-catalog-" + "api"
    old_stage = "stage" + ".py"
    old_validate_stage = "validate_" + old_stage

    assert not (root / api_dockerfile).exists()
    assert "FROM eclipse-temurin:17-jre-noble" in dockerfile
    assert '"pyspark==3.5.5"' in pyproject
    assert "ARG SPARK_VERSION=3.5.5" in dockerfile
    assert "ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.780" in dockerfile
    assert (
        "ARG AWS_JAVA_SDK_BUNDLE_SHA1=308a3af95a47e0c4e1f8bd98a37657d4661ae45e"
    ) in dockerfile
    assert "ARG AWS_SDK_V2_VERSION=2.29.52" in dockerfile
    assert (
        "ARG AWS_URL_CONNECTION_CLIENT_SHA1=b6732201e4ae7a2d9994c4b5bd3d3694551338c2"
    ) in dockerfile
    assert "aws-sdk-url-connection-client.jar" in dockerfile
    assert '"$SPARK_HOME/kubernetes/dockerfiles/spark/entrypoint.sh"' in dockerfile
    assert 'ENTRYPOINT ["/opt/entrypoint.sh"]' in dockerfile
    assert 'CMD ["video-media-catalog-community-spark", "--help"]' in dockerfile
    assert old_stage not in dockerfile
    assert old_validate_stage not in dockerfile
    assert "--extra index" in dockerfile

    assert emr_dockerfile.startswith(
        "FROM public.ecr.aws/emr-serverless/spark/emr-7.9.0:latest"
    )
    assert "python3.12" in emr_dockerfile
    assert "USER hadoop:hadoop" in emr_dockerfile

    retained = (
        "video-media-catalog-community-spark",
        "video-media-catalog-research-silver",
        "video-media-catalog-identity-curation",
        "video-media-catalog-gold-spark",
        "video-media-catalog-gold-index",
        "video-media-catalog-wikidata-full-media",
        "video-media-catalog-eidr-backfill",
        "video-media-catalog-europeana-oai",
        "video-media-catalog-imdb-sync",
        "video-media-catalog-tmdb-sync",
        "video-media-catalog-tvmaze-sync",
    )
    assert all(name in pyproject for name in retained)

    removed = (
        api_entrypoint,
        "video-media-catalog-" + "index =",
        "video-media-catalog-spark =",
        "video-media-catalog-validate",
        "video-media-catalog-" + "v1-adapter",
        "video-media-catalog-wikidata-" + "subset",
        "video-media-catalog-reference-" + "subset",
    )
    assert all(name not in pyproject for name in removed)
    assert "fast" + "api" not in pyproject
    assert "uvi" + "corn" not in pyproject
    assert "py" + "jwt" not in pyproject

    assert api_dockerfile not in publish_workflow
    assert api_entrypoint not in publish_workflow
    assert api_dockerfile not in ci_workflow
    assert "dockerfile: Dockerfile.emr" in publish_workflow
    assert "findingSeverityCounts.CRITICAL" in publish_workflow
