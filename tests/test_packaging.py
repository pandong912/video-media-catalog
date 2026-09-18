from __future__ import annotations

from pathlib import Path


def test_docker_and_python_spark_versions_are_aligned() -> None:
    root = Path(__file__).resolve().parents[1]
    pyproject = (root / "pyproject.toml").read_text()
    dockerfile = (root / "Dockerfile").read_text()

    assert '"pyspark==3.5.5"' in pyproject
    assert "ARG SPARK_VERSION=3.5.5" in dockerfile
    assert "ARG AWS_JAVA_SDK_BUNDLE_VERSION=1.12.780" in dockerfile
    assert (
        "ARG AWS_JAVA_SDK_BUNDLE_SHA1=308a3af95a47e0c4e1f8bd98a37657d4661ae45e"
    ) in dockerfile
    assert '"$SPARK_HOME/kubernetes/dockerfiles/spark/entrypoint.sh"' in dockerfile
    assert "JAVA_HOME=/opt/java" in dockerfile
    assert 'ENTRYPOINT ["/opt/entrypoint.sh"]' in dockerfile
    assert "COPY stage.py /opt/video-media-catalog/stage.py" in dockerfile
    assert "WORKDIR /opt/spark/work-dir" in dockerfile
