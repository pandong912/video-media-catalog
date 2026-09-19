"""Explicit Spark schemas for Gold v2 rows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from video_media_catalog.gold_tables import (
    GOLD_DATA_COLUMNS,
    GOLD_NULLABLE_COLUMNS,
    GOLD_TABLE_COLUMNS,
)

_LONG_COLUMNS = {("community_gold_entity", "source_node_count")}


def gold_table_schema(table: str):
    if table not in GOLD_TABLE_COLUMNS:
        raise KeyError(f"unknown Gold table: {table}")
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
    )

    fields = []
    for column in GOLD_TABLE_COLUMNS[table]:
        data_type = LongType() if (table, column) in _LONG_COLUMNS else StringType()
        fields.append(
            StructField(
                column,
                data_type,
                column in GOLD_NULLABLE_COLUMNS[table],
            )
        )
    return StructType(fields)


def create_gold_dataframes(
    spark: Any,
    rows: Mapping[str, Sequence[dict[str, Any]]],
) -> dict[str, Any]:
    if set(rows) != set(GOLD_DATA_COLUMNS):
        raise ValueError("all Gold data tables are required")
    return {
        table: spark.createDataFrame(
            list(rows[table]),
            schema=gold_table_schema(table),
        )
        for table in GOLD_DATA_COLUMNS
    }
