from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from video_media_catalog.attribution import (
    AttributionEntry,
    build_attribution_manifest,
)
from video_media_catalog.gold import (
    build_gold_release_plan,
    research_context,
    research_policy,
)
from video_media_catalog.gold_iceberg import CommunityGoldTables
from video_media_catalog.gold_ingest import (
    ATTRIBUTION_MEDIA_TYPE,
    GOLD_QUALITY_MEDIA_TYPE,
    GoldReleaseCommit,
)
from video_media_catalog.gold_quality import build_gold_quality_report
from video_media_catalog.gold_resolution import GoldResolutionDraft
from video_media_catalog.gold_tables import GOLD_TABLE_COLUMNS
from video_media_catalog.iceberg import CatalogConfig
from video_media_catalog.models import Checksum, ObjectRef

TIMESTAMP = "2026-09-19T00:00:00Z"


class FakeCatalog:
    def __init__(self) -> None:
        self.dropped = []

    def dropTempView(self, name: str) -> None:
        self.dropped.append(name)


class FakeConf:
    def __init__(self) -> None:
        self.values = {}

    def get(self, key: str, default: str) -> str:
        return self.values.get(key, default)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value


class FakeResult:
    def collect(self):
        return []


class FakeSpark:
    def __init__(self) -> None:
        self.statements = []
        self.catalog = FakeCatalog()
        self.conf = FakeConf()

    def sql(self, statement: str):
        self.statements.append(statement)
        return FakeResult()


class FakeFrame:
    def __init__(self, table: str, count: int) -> None:
        self.columns = list(GOLD_TABLE_COLUMNS[table])
        self._count = count
        self.view = None

    def select(self, *columns):
        assert columns == tuple(self.columns)
        return self

    def dropDuplicates(self, keys):
        assert len(keys) == 1
        return self

    def persist(self):
        return self

    def unpersist(self):
        return None

    def count(self):
        return self._count

    def where(self, predicate):
        return self

    def limit(self, count):
        assert count == 1
        self._count = 0
        return self

    def createOrReplaceTempView(self, view):
        self.view = view


def test_gold_tables_have_release_partition_and_numeric_count(
    tmp_path: Path,
) -> None:
    spark = FakeSpark()
    tables = CommunityGoldTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="community_gold",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    tables.create_tables()
    creates = [
        statement
        for statement in spark.statements
        if "CREATE TABLE IF NOT EXISTS" in statement
    ]
    assert len(creates) == len(GOLD_TABLE_COLUMNS)
    assert all("bucket(128, `release_plan_id`)" in sql for sql in creates)
    assert any("`source_node_count` BIGINT NOT NULL" in sql for sql in creates)


def test_gold_commit_is_unique_by_release_plan(tmp_path: Path) -> None:
    spark = FakeSpark()
    tables = CommunityGoldTables(
        spark,
        CatalogConfig(
            catalog_name="media",
            namespace="community_gold",
            warehouse=(tmp_path / "warehouse").as_uri(),
        ),
    )
    frame = FakeFrame("community_gold_release_commit", 1)
    assert tables.merge_insert_only("community_gold_release_commit", frame) == 1
    merge = spark.statements[-1]
    assert "ON t.`release_plan_id` = s.`release_plan_id`" in merge
    assert "WHEN MATCHED" not in merge


def _draft() -> GoldResolutionDraft:
    return GoldResolutionDraft(
        entity_keys=("sha256:" + ("1" * 64),),
        source_node_counts={"sha256:" + ("1" * 64): 1},
        fields=(),
        identifiers=(),
        relations=(),
        conflicts=(),
        eligible_policy_counts={"tvmaze-api-cc-by-sa": 1},
        withheld_assertion_count=0,
        unresolved_identity_count=0,
    )


def _plan():
    draft = _draft()
    policy = research_policy()
    return build_gold_release_plan(
        owner_subject="owner-123",
        policy_context=research_context(
            as_of=TIMESTAMP,
        ),
        committed_run_ids=("sha256:" + ("a" * 64),),
        silver_snapshot_ids={"community_field_assertion": 10},
        identity_snapshot_ids={"community_entity_membership": 11},
        rights_registry_digest="sha256:" + ("b" * 64),
        field_policy_digest=policy.digest,
        resolver_digest="sha256:" + ("c" * 64),
        image_digest="sha256:" + ("d" * 64),
        config_digest="sha256:" + ("e" * 64),
        expected_counts=draft.expected_counts,
        planned_at=TIMESTAMP,
    )


def _control_ref(payload: bytes, media_type: str, name: str) -> ObjectRef:
    return ObjectRef(
        uri=f"file:///tmp/{name}",
        format="OBJECT_FORMAT_JSON",
        media_type=media_type,
        checksum=Checksum(value=hashlib.sha256(payload).hexdigest()),
        size_bytes=len(payload),
    )


class StageFrame:
    def __init__(self, count: int, plan_id: str) -> None:
        self._count = count
        self.plan_id = plan_id

    def count(self):
        return self._count

    def where(self, predicate):
        expected = f"`release_plan_id` <> '{self.plan_id}'"
        return StageFrame(0 if expected in predicate else 1, self.plan_id)

    def limit(self, count):
        assert count == 1
        return self


class StageSpark:
    def createDataFrame(self, rows):
        return rows


class RecordingGoldTables(CommunityGoldTables):
    def __init__(self, counts) -> None:
        super().__init__(
            StageSpark(),
            CatalogConfig(
                catalog_name="media",
                namespace="community_gold",
                warehouse="file:///tmp/community-gold-test",
            ),
        )
        self.counts = counts
        self.events = []
        self.commit: GoldReleaseCommit | None = None
        self.snapshot_properties = {}
        self.foreign_latest = {}

    def create_tables(self):
        self.events.append("create")

    def read_commit(self, release_plan_id):
        return self.commit

    def merge_insert_only(
        self,
        table,
        dataframe,
        *,
        snapshot_properties=None,
    ):
        self.events.append(table)
        self.snapshot_properties[table] = snapshot_properties
        if table == "community_gold_release_commit":
            self.commit = GoldReleaseCommit.model_validate_json(
                dataframe[0]["commit_json"]
            )
        count = len(dataframe) if isinstance(dataframe, list) else dataframe.count()
        if table in self.counts and count:
            # Simulate a foreign writer advancing latest before snapshot lookup.
            self.foreign_latest[table] = 999
            self.events.append(f"foreign-writer:{table}")
        return count

    def _verify_plan(self, plan):
        self.events.append("verify-plan")

    def _plan_row_count(self, table, release_plan_id):
        return self.counts[table]

    def _release_snapshot_id(
        self,
        table,
        release_plan_id,
        *,
        expected_row_count,
    ):
        assert expected_row_count == self.counts[table]
        return 100 if expected_row_count else None

    def _latest_snapshot_id(self, table):
        raise AssertionError(
            "stage_and_commit must not read the global latest snapshot"
        )


def test_gold_commit_pins_own_snapshot_after_foreign_write_and_is_quality_gated() -> (
    None
):
    draft = _draft()
    plan = _plan()
    policy = research_policy()
    quality = build_gold_quality_report(
        plan=plan,
        draft=draft,
        policy=policy,
        created_at=TIMESTAMP,
    )
    attribution = build_attribution_manifest(
        release_id=plan.release_plan_id,
        entries=(
            AttributionEntry(
                source_product_id="tvmaze-public-api",
                policy_id="tvmaze-api-cc-by-sa",
                attribution_text="TV data provided by TVmaze.",
                license_id="CC-BY-SA",
                source_url="https://www.tvmaze.com/",
                claim_count=1,
            ),
        ),
        created_at=TIMESTAMP,
    )
    frames = {
        table: StageFrame(count, plan.release_plan_id)
        for table, count in plan.expected_counts.items()
    }
    tables = RecordingGoldTables(plan.expected_counts)
    mismatched_attribution = build_attribution_manifest(
        release_id=plan.release_plan_id,
        entries=(attribution.entries[0].model_copy(update={"claim_count": 2}),),
        created_at=TIMESTAMP,
    )
    with pytest.raises(ValueError, match="cover every eligible assertion"):
        tables.stage_and_commit(
            plan=plan,
            dataframes=frames,
            quality_report=quality,
            quality_report_ref=_control_ref(
                quality.json_bytes(), GOLD_QUALITY_MEDIA_TYPE, "quality.json"
            ),
            attribution_manifest=mismatched_attribution,
            attribution_manifest_ref=_control_ref(
                mismatched_attribution.json_bytes(),
                ATTRIBUTION_MEDIA_TYPE,
                "bad-attribution.json",
            ),
            committed_at=TIMESTAMP,
        )
    commit = tables.stage_and_commit(
        plan=plan,
        dataframes=frames,
        quality_report=quality,
        quality_report_ref=_control_ref(
            quality.json_bytes(), GOLD_QUALITY_MEDIA_TYPE, "quality.json"
        ),
        attribution_manifest=attribution,
        attribution_manifest_ref=_control_ref(
            attribution.json_bytes(),
            ATTRIBUTION_MEDIA_TYPE,
            "attribution.json",
        ),
        committed_at=TIMESTAMP,
    )
    assert commit.table_counts == plan.expected_counts
    assert commit.owner_subject == "owner-123"
    assert commit.context_id == "research"
    assert commit.table_snapshot_ids["community_gold_entity"] == 100
    assert tables.foreign_latest["community_gold_entity"] == 999
    assert tables.snapshot_properties["community_gold_entity"] == {
        "video-media-catalog.release-plan-id": plan.release_plan_id
    }
    assert tables.events[-1] == "community_gold_release_commit"
