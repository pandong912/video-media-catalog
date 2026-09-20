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
    assert "MAX_EXACT_BLOCKING_NODE_CANDIDATE_KEYS = 256" in source
    assert "MAX_EXACT_BLOCKING_COMPONENT_CANDIDATE_KEYS = 256" in source
    tree = ast.parse(source)
    function_names = {
        node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
    }
    assert "assign_exact_blocking_component_ids" in function_names
    assert "build_exact_blocking_component_stats" in function_names
    assert "build_parent_constrained_work" in function_names
    assert "_materialize_exact_blocking_labels" in function_names
    assert "class IdentityResolutionConfig" in source
    assert "identityResolutionConfigDigest" in source
    assert "conflictCountsByReason" in source


def test_identity_spark_reuses_single_lifecycle_projection() -> None:
    source = _identity_spark_source()
    assert "persist_latest_source_record_states" in source
    assert "current_envelope_keys_from_latest" in source
    assert "build_inactive_membership_revocation_worklist" in source
    assert "current_upsert_envelope_keys(" not in source
    assert "inactive_source_records(" not in source
    assert "latest_source_record_states.unpersist()" in source
    assert "bound_source_records.unpersist()" in source


def test_identity_spark_truncates_label_lineage_with_local_checkpoint() -> None:
    source = _identity_spark_source()
    assert "localCheckpoint(eager=True)" in source
    assert "refusing incomplete merge" in source
    assert 'countDistinct("candidate_entity_key")' in source
    assert "node_candidate_counts" in source
    assert (
        ".persist()"
        in source.split("node_candidate_counts", 1)[1].split("finally:", 1)[0]
    )
    assert "candidate_counts = distinct_candidates.groupBy" in source
    assert (
        "is_cached"
        not in source.split("def _release_exact_blocking_labels", 1)[1].split(
            "\ndef ", 1
        )[0]
    )
    assign_block = source.split("def assign_exact_blocking_component_ids", 1)[1]
    assign_block = assign_block.split("\ndef ", 1)[0]
    assert ".count()" not in assign_block
    assert ".take(1)" in assign_block
    assert "stable_nodes = _materialize_exact_blocking_labels(" in assign_block
    assert "stable_edges = _materialize_exact_blocking_labels(" in assign_block
    assert 'stable_edges.join(blocking_labels, "blocking_key")' in assign_block
    stats_block = source.split(
        "def build_exact_blocking_component_stats", 1
    )[1].split("\ndef ", 1)[0]
    assert ".rdd" not in stats_block
    assert ".groupBy(" in stats_block
    assert "count_component_id" in source
    assert "candidate_keys_component_id" in source
    materialize_index = assign_block.index(
        "_materialize_exact_blocking_labels(next_labels)"
    )
    release_index = assign_block.index("_release_exact_blocking_labels(previous)")
    assert materialize_index < release_index
    assert "finally:" in assign_block
    assert "oversized_node_keys" in source
    assert "bounded_node_keys.take(1)" not in source
    assert "CONFLICT_NODE_CANDIDATES" in source


def test_parent_membership_join_is_vectorized_and_ordered() -> None:
    source = _identity_spark_source()
    parent_block = source.split("def build_parent_constrained_work", 1)[1]
    parent_block = parent_block.split("\ndef ", 1)[0]
    assert ".rdd" not in parent_block
    assert "parent_memberships" in parent_block
    assert "resolved_parent_membership_count" in parent_block
    assert "PARENT_MEMBERSHIP_UNRESOLVED" in source
    assert "PARENT_MEMBERSHIP_AMBIGUOUS" in source
    assert source.index("season_results =") < source.index("episode_results =")
    assert "EXISTING_MEMBERSHIP_EXACT_ID_CONFLICT" in source
    assert '"membershipRewriteSuppressed": True' in source
