"""Distributed Wikidata full-media selection and envelope sharding."""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from video_media_catalog.community_sources import wikidata_rights_profile
from video_media_catalog.connector import (
    ChangeSemantics,
    Completeness,
    ConnectorBatchManifest,
    DeleteCoverage,
    RecordOperation,
    Serialization,
    SourceWindow,
    TransportKind,
    build_connector_batch_manifest,
    build_connector_record_envelope,
)
from video_media_catalog.constants import (
    CREDIT_ORGANIZATION_PROPERTIES,
    CREDIT_PERSON_PROPERTIES,
    MEDIA_ENTITY_TYPES,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.source_mappers import (
    WIKIDATA_FULL_MEDIA_CONNECTOR_ID,
    WIKIDATA_NAMESPACE_ID,
    WIKIDATA_SOURCE_PRODUCT_ID,
    WIKIDATA_SOURCE_SYSTEM_ID,
)
from video_media_catalog.v2_contracts import digest_identity
from video_media_catalog.wikidata_full_backfill import (
    FULL_MEDIA_PARENT_PROPERTIES,
    FULL_MEDIA_PAYLOAD_SCHEMA,
    FullMediaBackfillConfig,
    WikidataFullMediaProfile,
    full_media_build_digest,
    full_media_coverage_scope,
    full_media_dump_window,
)
from video_media_catalog.wikidata_spark import (
    _materialize,
    classify_entities,
)

_SELECTION_TYPES = (
    "MOVIE",
    "TV_SERIES",
    "TV_SEASON",
    "TV_EPISODE",
    "PERSON",
    "ORGANIZATION",
    "UNKNOWN",
)


@dataclass(frozen=True)
class SparkFullMediaBuild:
    batch: ConnectorBatchManifest
    profile: WikidataFullMediaProfile
    selected: Any
    envelopes: Any
    lines: Any
    shard_summaries: Any

    def unpersist(self) -> None:
        self.selected.unpersist()
        self.envelopes.unpersist()


def _selection_columns(frame: Any) -> Any:
    return frame.select(
        "qid",
        "qid_numeric",
        "payload_json",
        "relations",
        "entity_type",
        "selection_role",
    )


def select_full_media_entities(
    spark: Any,
    normalized: Any,
    *,
    max_closure_iterations: int,
) -> tuple[Any, Any]:
    """Select every media root plus distributed parent and credit closure."""

    from pyspark.sql import functions as F

    if max_closure_iterations < 1:
        raise ValueError("max_closure_iterations must be positive")
    entity_types, type_closure = classify_entities(
        spark,
        normalized,
        max_closure_iterations=max_closure_iterations,
    )
    candidates = (
        normalized.select(
            "qid",
            "qid_numeric",
            "payload_json",
            "relations",
        )
        .join(entity_types, "qid", "inner")
        .persist()
    )
    relation_edges = _materialize(
        normalized.select(
            F.col("qid").alias("subject_qid"),
            F.col("qid_numeric").alias("subject_qid_numeric"),
            F.explode("relations").alias("relation"),
        ).select(
            "subject_qid",
            "subject_qid_numeric",
            F.col("relation.property_id").alias("property_id"),
            F.col("relation.target_qid").alias("target_qid"),
            F.col("relation.target_type_hint").alias("target_type_hint"),
        )
    )
    selected = _materialize(
        candidates.where(F.col("entity_type").isin(sorted(MEDIA_ENTITY_TYPES)))
        .withColumn("selection_role", F.lit("MEDIA"))
        .transform(_selection_columns)
    )
    parent_edges = relation_edges.where(
        F.col("property_id").isin(sorted(FULL_MEDIA_PARENT_PROPERTIES))
    )
    for _ in range(max_closure_iterations):
        parent_qids = (
            selected.select(F.col("qid").alias("subject_qid"))
            .join(parent_edges, "subject_qid", "inner")
            .select(F.col("target_qid").alias("qid"))
            .dropDuplicates(["qid"])
        )
        delta = (
            candidates.join(parent_qids, "qid", "inner")
            .join(selected.select("qid"), "qid", "left_anti")
            .withColumn("selection_role", F.lit("PARENT"))
            .transform(_selection_columns)
        )
        if delta.count() == 0:
            break
        selected = _materialize(selected.unionByName(delta))
    else:
        candidates.unpersist()
        entity_types.unpersist()
        type_closure.unpersist()
        raise RuntimeError(
            "full-media parent closure did not converge within "
            f"{max_closure_iterations} iterations"
        )

    credit_properties = sorted(
        set(CREDIT_PERSON_PROPERTIES) | set(CREDIT_ORGANIZATION_PROPERTIES)
    )
    credit_hints = (
        selected.select(F.col("qid").alias("subject_qid"))
        .join(
            relation_edges.where(F.col("property_id").isin(credit_properties)),
            "subject_qid",
            "inner",
        )
        .select(
            F.col("target_qid").alias("qid"),
            F.when(
                F.col("property_id").isin(sorted(CREDIT_PERSON_PROPERTIES)),
                F.struct(F.lit(0).alias("priority"), F.lit("PERSON").alias("kind")),
            )
            .otherwise(
                F.struct(
                    F.lit(1).alias("priority"),
                    F.lit("ORGANIZATION").alias("kind"),
                )
            )
            .alias("hint"),
        )
        .groupBy("qid")
        .agg(F.min("hint").alias("hint"))
        .select("qid", F.col("hint.kind").alias("hint_type"))
    )
    credit_rows = (
        candidates.join(credit_hints, "qid", "inner")
        .join(selected.select("qid"), "qid", "left_anti")
        .withColumn(
            "entity_type",
            F.when(F.col("entity_type") == "UNKNOWN", F.col("hint_type")).otherwise(
                F.col("entity_type")
            ),
        )
        .drop("hint_type")
        .withColumn("selection_role", F.lit("CREDIT"))
        .transform(_selection_columns)
    )
    selected = _materialize(selected.unionByName(credit_rows)).persist()
    candidates.unpersist()
    entity_types.unpersist()
    type_closure.unpersist()
    return selected, relation_edges


def _selection_metrics(selected: Any, relation_edges: Any) -> dict[str, Any]:
    from pyspark.sql import functions as F

    expressions = [F.count(F.lit(1)).alias("record_count")]
    expressions.extend(
        F.sum(
            F.when(
                (F.col("selection_role") == "MEDIA")
                & (F.col("entity_type") == entity_type),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias(f"root_{entity_type}")
        for entity_type in sorted(MEDIA_ENTITY_TYPES)
    )
    expressions.extend(
        F.sum(
            F.when(F.col("entity_type") == entity_type, F.lit(1)).otherwise(F.lit(0))
        ).alias(f"selected_{entity_type}")
        for entity_type in _SELECTION_TYPES
    )
    expressions.extend(
        (
            F.sum(
                F.when(F.col("selection_role") == "PARENT", F.lit(1)).otherwise(
                    F.lit(0)
                )
            ).alias("parent_count"),
            F.sum(
                F.when(
                    (F.col("selection_role") == "CREDIT")
                    & (F.col("entity_type") == "PERSON"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("credit_person_count"),
            F.sum(
                F.when(
                    (F.col("selection_role") == "CREDIT")
                    & (F.col("entity_type") == "ORGANIZATION"),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("credit_organization_count"),
        )
    )
    row = selected.agg(*expressions).first()
    if row is None or int(row["record_count"]) < 1:
        raise ValueError("full-media selector found no records")

    selected_edges = relation_edges.join(
        selected.select(F.col("qid").alias("subject_qid")),
        "subject_qid",
        "inner",
    )
    credit_properties = sorted(
        set(CREDIT_PERSON_PROPERTIES) | set(CREDIT_ORGANIZATION_PROPERTIES)
    )
    edge_row = selected_edges.agg(
        F.count(F.lit(1)).alias("relation_edge_count"),
        F.sum(
            F.when(
                F.col("property_id").isin(sorted(FULL_MEDIA_PARENT_PROPERTIES)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("parent_edge_count"),
        F.sum(
            F.when(
                F.col("property_id").isin(credit_properties),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("credit_edge_count"),
    ).first()
    assert edge_row is not None
    return {
        "record_count": int(row["record_count"]),
        "root_counts": {
            entity_type: int(row[f"root_{entity_type}"] or 0)
            for entity_type in sorted(MEDIA_ENTITY_TYPES)
        },
        "selected_type_counts": {
            entity_type: int(row[f"selected_{entity_type}"] or 0)
            for entity_type in _SELECTION_TYPES
        },
        "parent_count": int(row["parent_count"] or 0),
        "credit_person_count": int(row["credit_person_count"] or 0),
        "credit_organization_count": int(row["credit_organization_count"] or 0),
        "relation_edge_count": int(edge_row["relation_edge_count"] or 0),
        "parent_edge_count": int(edge_row["parent_edge_count"] or 0),
        "credit_edge_count": int(edge_row["credit_edge_count"] or 0),
    }


def _envelope_rows(
    rows: Iterable[Any],
    *,
    batch_payload: str,
) -> Iterator[dict[str, Any]]:
    batch = ConnectorBatchManifest.model_validate_json(batch_payload)
    raw_object = batch.raw_objects[0]
    for row in rows:
        payload = json.loads(row["payload_json"])
        entity_type = str(row["entity_type"])
        if entity_type != "UNKNOWN":
            payload["entityTypeHint"] = entity_type
        revision = payload.get("lastrevid")
        modified = payload.get("modified")
        qid = str(row["qid"])
        envelope = build_connector_record_envelope(
            payload=payload,
            batch_id=batch.batch_id,
            source_system_id=batch.source_system_id,
            source_product_id=batch.source_product_id,
            source_namespace_id=WIKIDATA_NAMESPACE_ID,
            source_record_id=qid,
            source_revision=None if revision is None else str(revision),
            operation=RecordOperation.UPSERT,
            source_modified_at=modified if isinstance(modified, str) else None,
            observed_at=batch.acquired_at,
            ingested_at=batch.acquired_at,
            payload_schema=FULL_MEDIA_PAYLOAD_SCHEMA,
            raw_object=raw_object,
            source_location=f"/entities/{qid}",
            policy_id=batch.policy_id,
            policy_digest=batch.policy_digest,
        )
        encoded = envelope.json_bytes()
        yield {
            "qid": qid,
            "qid_numeric": int(row["qid_numeric"]),
            "entity_type": entity_type,
            "selection_role": str(row["selection_role"]),
            "envelope_key": envelope.envelope_key,
            "value": encoded[:-1].decode("utf-8"),
            "envelope_bytes": len(encoded),
        }


def _envelope_schema() -> Any:
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("qid", StringType(), False),
            StructField("qid_numeric", LongType(), False),
            StructField("entity_type", StringType(), False),
            StructField("selection_role", StringType(), False),
            StructField("envelope_key", StringType(), False),
            StructField("value", StringType(), False),
            StructField("envelope_bytes", IntegerType(), False),
        ]
    )


def _build_envelopes(
    spark: Any,
    selected: Any,
    batch: ConnectorBatchManifest,
) -> Any:
    payload = batch.model_dump_json(by_alias=True, exclude_none=True)
    return spark.createDataFrame(
        selected.select(
            "qid",
            "qid_numeric",
            "payload_json",
            "entity_type",
            "selection_role",
        ).rdd.mapPartitions(lambda rows: _envelope_rows(rows, batch_payload=payload)),
        schema=_envelope_schema(),
    ).persist()


def build_full_media_backfill(
    spark: Any,
    normalized: Any,
    *,
    dump: ObjectRef,
    config: FullMediaBackfillConfig,
    image_digest: str,
    dump_date: str,
    acquired_at: str,
) -> SparkFullMediaBuild:
    """Build exact profile metrics and distributed replayable envelopes."""

    from pyspark.sql import functions as F

    selected, relation_edges = select_full_media_entities(
        spark,
        normalized,
        max_closure_iterations=config.max_closure_iterations,
    )
    metrics = _selection_metrics(selected, relation_edges)
    relation_edges.unpersist()
    policy = wikidata_rights_profile()
    coverage_scope = full_media_coverage_scope()
    window_start, window_end = full_media_dump_window(dump_date)
    batch = build_connector_batch_manifest(
        source_system_id=WIKIDATA_SOURCE_SYSTEM_ID,
        source_product_id=WIKIDATA_SOURCE_PRODUCT_ID,
        connector_id=WIKIDATA_FULL_MEDIA_CONNECTOR_ID,
        connector_version="1.0.0",
        image_digest=image_digest,
        config_digest=config.digest,
        policy_id=policy.policy_id,
        policy_digest=policy.digest,
        transport_kind=TransportKind.DUMP,
        serialization=Serialization.JSON_LINES,
        change_semantics=ChangeSemantics.FULL_SNAPSHOT,
        completeness=Completeness.COMPLETE,
        delete_coverage=DeleteCoverage.SNAPSHOT_DIFF,
        coverage_scope=coverage_scope,
        source_window=SourceWindow(start=window_start, end=window_end),
        watermark_after=dump_date,
        raw_objects=(dump,),
        acquired_at=acquired_at,
        record_count=metrics["record_count"],
        error_count=0,
    )
    envelopes = _build_envelopes(spark, selected, batch)
    byte_row = envelopes.agg(
        F.count(F.lit(1)).alias("record_count"),
        F.sum("envelope_bytes").alias("envelope_bytes"),
    ).first()
    assert byte_row is not None
    if int(byte_row["record_count"]) != metrics["record_count"]:
        raise RuntimeError("envelope count differs from full-media selection")
    envelope_bytes = int(byte_row["envelope_bytes"] or 0)
    if envelope_bytes < 1:
        raise RuntimeError("full-media envelopes have no bytes")
    estimated_shards = min(
        metrics["record_count"],
        max(1, math.ceil(envelope_bytes / config.target_shard_bytes)),
    )
    if estimated_shards > config.max_supported_shards:
        raise ValueError("estimated shard count exceeds epoch/partition capacity")
    estimated_partitions = math.ceil(estimated_shards / config.max_shards_per_partition)
    estimated_epochs = math.ceil(estimated_partitions / config.max_partitions_per_epoch)
    build_digest = full_media_build_digest(
        dump=dump,
        config_digest=config.digest,
        image_digest=image_digest,
    )
    profile = WikidataFullMediaProfile(
        build_digest=build_digest,
        config_digest=config.digest,
        image_digest=image_digest,
        batch_id=batch.batch_id,
        dump=dump,
        coverage_scope=coverage_scope,
        coverage_scope_digest=digest_identity(coverage_scope),
        root_counts=metrics["root_counts"],
        selected_type_counts=metrics["selected_type_counts"],
        parent_count=metrics["parent_count"],
        credit_person_count=metrics["credit_person_count"],
        credit_organization_count=metrics["credit_organization_count"],
        record_count=metrics["record_count"],
        relation_edge_count=metrics["relation_edge_count"],
        parent_edge_count=metrics["parent_edge_count"],
        credit_edge_count=metrics["credit_edge_count"],
        envelope_bytes=envelope_bytes,
        estimated_shards=estimated_shards,
        estimated_partitions=estimated_partitions,
        estimated_epochs=estimated_epochs,
    )

    sharded = (
        envelopes.withColumn(
            "shard_index",
            F.pmod(F.col("qid_numeric"), F.lit(estimated_shards)).cast("int"),
        )
        .withColumn(
            "global_partition_index",
            F.floor(F.col("shard_index") / F.lit(config.max_shards_per_partition)).cast(
                "int"
            ),
        )
        .withColumn(
            "epoch_index",
            F.floor(
                F.col("global_partition_index") / F.lit(config.max_partitions_per_epoch)
            ).cast("int"),
        )
        .withColumn(
            "partition_index",
            F.pmod(
                F.col("global_partition_index"),
                F.lit(config.max_partitions_per_epoch),
            ).cast("int"),
        )
    )
    shard_summaries = sharded.groupBy(
        "epoch_index",
        "partition_index",
        "global_partition_index",
        "shard_index",
    ).agg(
        F.count(F.lit(1)).alias("record_count"),
        F.sum("envelope_bytes").alias("size_bytes"),
        F.min_by("envelope_key", "qid_numeric").alias("first_envelope_key"),
        F.max_by("envelope_key", "qid_numeric").alias("last_envelope_key"),
    )
    lines = (
        sharded.repartition(estimated_shards, "shard_index")
        .sortWithinPartitions("shard_index", "qid_numeric")
        .select("shard_index", "value")
    )
    return SparkFullMediaBuild(
        batch=batch,
        profile=profile,
        selected=selected,
        envelopes=envelopes,
        lines=lines,
        shard_summaries=shard_summaries,
    )


def write_full_media_staging(
    build: SparkFullMediaBuild,
    *,
    record_uri: str,
    summary_uri: str,
) -> Any:
    """Write distributed shards and durable summaries, returning pinned summaries."""

    (
        build.lines.write.mode("errorifexists")
        .partitionBy("shard_index")
        .text(record_uri)
    )
    build.shard_summaries.write.mode("errorifexists").parquet(summary_uri)
    summaries = build.shard_summaries.sparkSession.read.parquet(summary_uri).persist()
    count = summaries.count()
    if count < 1 or count > build.profile.estimated_shards:
        summaries.unpersist()
        raise RuntimeError("materialized shard summary count is out of bounds")
    return summaries
