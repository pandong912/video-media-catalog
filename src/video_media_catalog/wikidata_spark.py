"""Distributed Wikidata normalization and media-type classification."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping
from typing import Any, Literal, Self

from pydantic import Field, field_serializer, model_validator

from video_media_catalog.canonical import canonical_json
from video_media_catalog.constants import (
    CREDIT_ORGANIZATION_PROPERTIES,
    CREDIT_PERSON_PROPERTIES,
    ENTITY_TYPE_SEEDS,
    RELATION_PROPERTIES,
)
from video_media_catalog.models import ObjectRef
from video_media_catalog.source_mappers import qid_values, statement_value, statements
from video_media_catalog.v2_contracts import V2ContractModel
from video_media_catalog.wikidata import normalize_wikidata_entity

NORMALIZATION_ALGORITHM_ID = "video-media-catalog-wikidata-normalization-v1"
_MATERIALIZE_SCRATCH_CONF = "spark.video_media_catalog.materializeScratchUri"
_QID = re.compile(r"Q[1-9][0-9]*")
_TYPE_PRIORITY = {
    "TV_EPISODE": 0,
    "TV_SEASON": 1,
    "TV_SERIES": 2,
    "MOVIE": 3,
    "PERSON": 4,
    "ORGANIZATION": 5,
    "UNKNOWN": 99,
}


def wikipedia_sitelink_count(payload: Mapping[str, Any]) -> int:
    """Count language-Wikipedia sitelinks, excluding non-Wikipedia projects."""

    sitelinks = payload.get("sitelinks")
    if not isinstance(sitelinks, Mapping):
        return 0
    excluded = {
        "commonswiki",
        "incubatorwiki",
        "mediawiki",
        "metawiki",
        "specieswiki",
        "testwiki",
        "wikidatawiki",
    }
    return sum(
        1
        for site in sitelinks
        if isinstance(site, str)
        and re.fullmatch(r"[a-z0-9_-]+wiki", site) is not None
        and site not in excluded
    )


def parse_and_normalize_dump_line(raw_line: str) -> dict[str, Any] | None:
    """Parse one official array line without relying on its dump position."""

    text = raw_line.strip().lstrip("\ufeff")
    if not text:
        return None
    if text.startswith("["):
        text = text[1:].lstrip()
    if text.endswith("]"):
        text = text[:-1].rstrip()
    if text.endswith(","):
        text = text[:-1].rstrip()
    if not text:
        return None
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("Wikidata dump line must contain one JSON object")
    return normalize_wikidata_entity(value)


class NormalizedStagingManifest(V2ContractModel):
    """Commit marker for reusable deterministic normalized Parquet staging."""

    schema_version: Literal["1.0"] = "1.0"
    status: Literal["COMPLETE"] = "COMPLETE"
    algorithm_id: Literal["video-media-catalog-wikidata-normalization-v1"] = (
        NORMALIZATION_ALGORITHM_ID
    )
    dump: ObjectRef
    data_uri: str
    row_count: int = Field(gt=0)

    @model_validator(mode="after")
    def require_immutable_dump(self) -> Self:
        if self.dump.etag is None or self.dump.object_version is None:
            raise ValueError("staging dump ObjectRef requires ETag and VersionId")
        return self

    @field_serializer("row_count", when_used="json")
    def serialize_row_count(self, value: int) -> str:
        return str(value)


def _normalized_dump_row(raw_line: str) -> dict[str, Any] | None:
    payload = parse_and_normalize_dump_line(raw_line)
    if payload is None:
        return None
    qid = str(payload.get("id") or "")
    if _QID.fullmatch(qid) is None:
        return None
    relations: list[dict[str, str]] = []
    for property_id in RELATION_PROPERTIES:
        for statement in statements(payload, property_id):
            target = statement_value(statement)
            if not isinstance(target, str) or _QID.fullmatch(target) is None:
                continue
            hint = (
                "PERSON"
                if property_id in CREDIT_PERSON_PROPERTIES
                else (
                    "ORGANIZATION"
                    if property_id in CREDIT_ORGANIZATION_PROPERTIES
                    else "UNKNOWN"
                )
            )
            relations.append(
                {
                    "property_id": property_id,
                    "target_qid": target,
                    "target_type_hint": hint,
                }
            )
    return {
        "qid": qid,
        "qid_numeric": int(qid[1:]),
        "payload_json": canonical_json(payload),
        "sitelink_count": wikipedia_sitelink_count(payload),
        "direct_types": sorted(set(qid_values(payload, "P31"))),
        "subclass_parents": sorted(set(qid_values(payload, "P279"))),
        "relations": relations,
    }


def item_entity_rows(normalized: Any) -> Any:
    """Keep only item QIDs so reused staging cannot expose property entities."""

    from pyspark.sql import functions as F

    return normalized.where(F.col("qid").rlike(r"^Q[1-9][0-9]*$"))


def normalized_schema() -> Any:
    from pyspark.sql.types import (
        ArrayType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    relation = StructType(
        [
            StructField("property_id", StringType(), False),
            StructField("target_qid", StringType(), False),
            StructField("target_type_hint", StringType(), False),
        ]
    )
    return StructType(
        [
            StructField("qid", StringType(), False),
            StructField("qid_numeric", LongType(), False),
            StructField("payload_json", StringType(), False),
            StructField("sitelink_count", LongType(), False),
            StructField("direct_types", ArrayType(StringType(), False), False),
            StructField("subclass_parents", ArrayType(StringType(), False), False),
            StructField("relations", ArrayType(relation, False), False),
        ]
    )


def normalize_dump(spark: Any, dump_uri: str) -> Any:
    """Read official line JSON with Spark and normalize without driver collect."""

    parsed = (
        spark.read.text(dump_uri)
        .rdd.map(lambda row: _normalized_dump_row(row["value"]))
        .filter(lambda row: row is not None)
    )
    normalized = spark.createDataFrame(parsed, schema=normalized_schema()).persist()
    if not normalized.take(1):
        normalized.unpersist()
        raise ValueError("Wikidata dump contains no entities")
    duplicate = (
        normalized.groupBy("qid").count().where("count > 1").select("qid").take(1)
    )
    if duplicate:
        normalized.unpersist()
        raise ValueError(f"Wikidata dump contains duplicate entity {duplicate[0].qid}")
    return normalized


def write_normalized_staging(normalized: Any, destination_uri: str) -> int:
    """Write reusable normalized Parquet staging with no overwrite."""

    row_count = normalized.count()
    (
        normalized.repartitionByRange("qid_numeric")
        .sortWithinPartitions("qid_numeric")
        .write.mode("errorifexists")
        .parquet(destination_uri)
    )
    return row_count


def _choose_type_expression(column: Any) -> Any:
    from pyspark.sql import functions as F

    priority = F.create_map(
        *[
            item
            for entity_type, number in _TYPE_PRIORITY.items()
            for item in (F.lit(entity_type), F.lit(number))
        ]
    )
    return F.struct(
        priority[column].alias("priority"),
        column.alias("entity_type"),
    )


def configure_bfs_materialize_dir(spark: Any, uri: str) -> None:
    """Set durable scratch URI used to truncate BFS lineage between hops."""

    cleaned = uri.strip()
    if not cleaned:
        raise ValueError("BFS materialize scratch URI must be non-empty")
    spark.conf.set(_MATERIALIZE_SCRATCH_CONF, cleaned.rstrip("/"))


def _materialize(frame: Any) -> Any:
    """Truncate Spark lineage through durable Parquet scratch storage."""

    spark = frame.sparkSession
    root = spark.conf.get(_MATERIALIZE_SCRATCH_CONF, None)
    if not root:
        raise RuntimeError(
            "BFS materialize scratch URI is not configured; call "
            "configure_bfs_materialize_dir(...) first"
        )
    path = f"{root}/mat-{uuid.uuid4().hex}"
    frame.write.mode("errorifexists").parquet(path)
    if getattr(frame, "is_cached", False):
        frame.unpersist()
    return spark.read.parquet(path)


def classify_entities(
    spark: Any,
    normalized: Any,
    *,
    max_closure_iterations: int,
) -> tuple[Any, Any]:
    """Apply the P31/P279 seed and precedence semantics."""

    from pyspark.sql import functions as F

    closure = _materialize(
        spark.createDataFrame(
            sorted(ENTITY_TYPE_SEEDS.items()), ["class_id", "entity_type"]
        )
    )
    edges = _materialize(
        normalized.select(
            F.col("qid").alias("child"),
            F.explode("subclass_parents").alias("parent"),
        ).dropDuplicates()
    )
    for _ in range(max_closure_iterations):
        delta = (
            edges.join(closure, edges.parent == closure.class_id, "inner")
            .select(F.col("child").alias("class_id"), "entity_type")
            .dropDuplicates()
            .join(closure, ["class_id", "entity_type"], "left_anti")
        )
        if delta.count() == 0:
            break
        closure = _materialize(closure.unionByName(delta).dropDuplicates())
    else:
        raise RuntimeError(
            "P31/P279 closure did not converge within "
            f"{max_closure_iterations} iterations"
        )

    direct = normalized.select(
        "qid",
        F.explode("direct_types").alias("class_id"),
    )
    classified = (
        direct.join(closure, "class_id", "inner")
        .select("qid", "entity_type")
        .dropDuplicates()
        .groupBy("qid")
        .agg(F.min(_choose_type_expression(F.col("entity_type"))).alias("chosen"))
        .select("qid", F.col("chosen.entity_type").alias("entity_type"))
    )
    entity_types = _materialize(
        normalized.select("qid")
        .join(classified, "qid", "left")
        .fillna({"entity_type": "UNKNOWN"})
    )
    return entity_types, closure
