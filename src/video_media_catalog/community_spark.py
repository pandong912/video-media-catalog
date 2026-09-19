"""Explicit Spark schemas for community Silver v2 rows."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from video_media_catalog.community_tables import (
    DATA_TABLE_COLUMNS,
    NULLABLE_COLUMNS,
    TABLE_COLUMNS,
)

_BOOLEAN_COLUMNS = {("community_entity_ledger", "imported_v1")}
_DOUBLE_COLUMNS = {("community_identity_evidence", "confidence")}


def community_table_schema(table: str):
    if table not in TABLE_COLUMNS:
        raise KeyError(f"unknown community catalog table: {table}")
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    fields = []
    for column in TABLE_COLUMNS[table]:
        if (table, column) in _BOOLEAN_COLUMNS:
            data_type = BooleanType()
        elif (table, column) in _DOUBLE_COLUMNS:
            data_type = DoubleType()
        else:
            data_type = StringType()
        fields.append(
            StructField(
                column,
                data_type,
                column in NULLABLE_COLUMNS[table],
            )
        )
    return StructType(fields)


def create_community_dataframes(
    spark: Any,
    rows: Mapping[str, Sequence[dict[str, Any]]],
) -> dict[str, Any]:
    if set(rows) != set(DATA_TABLE_COLUMNS):
        raise ValueError("all community Silver data tables are required")
    return {
        table: spark.createDataFrame(
            list(rows[table]),
            schema=community_table_schema(table),
        )
        for table in DATA_TABLE_COLUMNS
    }
