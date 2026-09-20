from __future__ import annotations

import ast
import re
from pathlib import Path


def _identity_spark_source() -> str:
    return (
        Path(__file__).resolve().parents[1]
        / "src"
        / "video_media_catalog"
        / "identity_spark.py"
    ).read_text(encoding="utf-8")


def test_identity_spark_avoids_driver_side_graph_materialization() -> None:
    source = _identity_spark_source()
    assert ".toLocalIterator(" not in source
    assert ".groupByKey(" not in source
    assert not re.search(r"(?<!\.)collect\(", source), (
        "identity_spark must not call DataFrame/RDD collect() for graph assembly"
    )


def test_identity_spark_exposes_bounded_component_limits() -> None:
    source = _identity_spark_source()
    assert "MAX_EXACT_BLOCKING_LABEL_ITERATIONS = 64" in source
    assert "MAX_EXACT_BLOCKING_RESOLUTION_COMPONENT_SIZE = 256" in source
    tree = ast.parse(source)
    function_names = {
        node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
    }
    assert "assign_exact_blocking_component_ids" in function_names
