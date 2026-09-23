from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession

from video_media_catalog.source_lifecycle import (
    current_envelope_keys_from_latest,
    filter_assertions_for_current_envelopes,
    latest_source_record_states,
)

OLD_RUN = "sha256:" + ("1" * 64)
NEW_RUN = "sha256:" + ("2" * 64)
ENVELOPE = "sha256:" + ("a" * 64)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("source-lifecycle-republish")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def _event(run_id: str, started_at: str) -> dict[str, str | None]:
    return {
        "envelope_key": ENVELOPE,
        "run_id": run_id,
        "source_system_id": "imdb",
        "source_product_id": "imdb-official-tsv",
        "source_namespace_id": "imdb-title",
        "source_record_id": "title.basics:tt1",
        "operation": "UPSERT",
        "observed_at": "2026-09-01T00:00:00Z",
        "ingested_at": "2026-09-01T00:00:00Z",
        "valid_from": None,
        "valid_to": None,
        "expires_at": None,
        "batch_id": "batch-1",
        "_run_started_at": started_at,
    }


def test_republished_envelope_keeps_later_mapper_run(spark: SparkSession) -> None:
    events = spark.createDataFrame(
        [
            _event(OLD_RUN, "2026-09-01T00:00:00Z"),
            _event(NEW_RUN, "2026-09-02T00:00:00Z"),
        ],
        """
        envelope_key STRING, run_id STRING, source_system_id STRING,
        source_product_id STRING, source_namespace_id STRING,
        source_record_id STRING, operation STRING, observed_at STRING,
        ingested_at STRING, valid_from STRING, valid_to STRING,
        expires_at STRING, batch_id STRING, _run_started_at STRING
        """,
    )
    latest = latest_source_record_states(events, as_of="2026-09-03T00:00:00Z")
    rows = latest.collect()
    assert len(rows) == 1
    assert rows[0]["run_id"] == NEW_RUN

    current = current_envelope_keys_from_latest(
        latest,
        as_of="2026-09-03T00:00:00Z",
    )
    assertions = spark.createDataFrame(
        [
            (
                "old",
                OLD_RUN,
                "ACTIVE",
                f'{{"envelopeKey":"{ENVELOPE}"}}',
                "TV_EPISODE",
            ),
            (
                "new",
                NEW_RUN,
                "ACTIVE",
                f'{{"envelopeKey":"{ENVELOPE}"}}',
                "MOVIE",
            ),
        ],
        "assertion_id STRING, run_id STRING, status STRING, "
        "provenance_json STRING, entity_type STRING",
    )
    visible = filter_assertions_for_current_envelopes(
        assertions,
        current_envelope_keys=current,
        as_of="2026-09-03T00:00:00Z",
    ).collect()
    assert [row["assertion_id"] for row in visible] == ["new"]
    assert [row["entity_type"] for row in visible] == ["MOVIE"]
