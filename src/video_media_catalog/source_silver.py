"""Generic connector-record projection into Silver v2 rows and Spark frames."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from typing import Any
from urllib.parse import urlsplit

from video_media_catalog.community_ingest import (
    CommunityIngestRun,
    IngestRunKind,
    build_community_ingest_run,
)
from video_media_catalog.community_rows import (
    empty_data_rows,
    entity_type_assertion_row,
    field_assertion_row,
    identifier_assertion_row,
    relationship_assertion_row,
    source_record_row,
    validate_data_rows,
)
from video_media_catalog.community_spark import community_table_schema
from video_media_catalog.community_tables import DATA_TABLE_COLUMNS
from video_media_catalog.connector import (
    ConnectorBatchManifest,
    ConnectorRecordEnvelope,
    ConnectorRecordSetManifest,
    validate_envelope_against_batch,
    validate_envelopes_against_batch,
)
from video_media_catalog.record_shard_materialization import (
    MaterializedRecordShard,
    bind_materialized_shards,
    materialized_shard_from_reference,
    validate_stream_envelope_key_bounds,
)
from video_media_catalog.source_mapper import MappedAssertions
from video_media_catalog.source_registry import (
    RegistryStatus,
    SourceRegistrySnapshot,
)
from video_media_catalog.storage import local_path

Mapper = Callable[[ConnectorRecordEnvelope], MappedAssertions]


def _spark_input_uri(uri: str) -> str:
    """Decode local file URIs before handing them to Hadoop's path parser."""

    return str(local_path(uri)) if urlsplit(uri).scheme == "file" else uri


def mapper_for_product(source_product_id: str) -> Mapper:
    """Resolve only reviewed, explicitly registered source products."""

    from video_media_catalog.imdb import (
        IMDB_SOURCE_PRODUCT_ID,
        map_imdb_record,
    )
    from video_media_catalog.tmdb import (
        TMDB_SOURCE_PRODUCT_ID,
        map_tmdb_record,
    )
    from video_media_catalog.tvmaze import (
        TVMAZE_SOURCE_PRODUCT_ID,
        map_tvmaze_show,
    )
    from video_media_catalog.v1_adapters import (
        EIDR_SOURCE_PRODUCT_ID,
        WIKIDATA_SOURCE_PRODUCT_ID,
        map_eidr_record,
        map_wikidata_record,
    )

    registry: dict[str, Mapper] = {
        TVMAZE_SOURCE_PRODUCT_ID: map_tvmaze_show,
        WIKIDATA_SOURCE_PRODUCT_ID: map_wikidata_record,
        EIDR_SOURCE_PRODUCT_ID: map_eidr_record,
        IMDB_SOURCE_PRODUCT_ID: map_imdb_record,
        TMDB_SOURCE_PRODUCT_ID: map_tmdb_record,
    }
    try:
        return registry[source_product_id]
    except KeyError as exc:
        raise ValueError(
            f"unsupported community source product: {source_product_id}"
        ) from exc


def _validate_inputs(
    registry: SourceRegistrySnapshot,
    batch: ConnectorBatchManifest,
    record_set: ConnectorRecordSetManifest,
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    systems = {item.source_system_id: item for item in registry.source_systems}
    products = {item.source_product_id: item for item in registry.source_products}
    policies = {item.policy_id: item for item in registry.rights_profiles}
    system = systems.get(batch.source_system_id)
    product = products.get(batch.source_product_id)
    if system is None or system.status != RegistryStatus.ACTIVE:
        raise ValueError("batch source system is not active in the pinned registry")
    if product is None or product.status != RegistryStatus.ACTIVE:
        raise ValueError("batch source product is not active in the pinned registry")
    if product.source_system_id != batch.source_system_id:
        raise ValueError("batch source product does not belong to its source system")
    if batch.connector_id not in product.connector_ids:
        raise ValueError("batch connector is not registered for its source product")
    if product.policy_id != batch.policy_id:
        raise ValueError("batch does not bind the source product rights policy")
    profile = policies.get(product.policy_id)
    if profile is None or profile.digest != batch.policy_digest:
        raise ValueError(
            "batch rights policy is absent or changed in the pinned registry"
        )
    if (
        record_set.source_product_id != batch.source_product_id
        or record_set.batch_id != batch.batch_id
        or record_set.record_count != batch.record_count
        or record_set.policy_id != batch.policy_id
        or record_set.policy_digest != batch.policy_digest
    ):
        raise ValueError("record set does not bind its connector batch")
    return (
        {
            item.namespace_id: item.source_product_id
            for item in registry.source_namespaces
        },
        {item.source_product_id: item.policy_id for item in registry.source_products},
        {item.policy_id: item.digest for item in registry.rights_profiles},
    )


def _validate_envelope_registry(
    batch: ConnectorBatchManifest,
    envelope: ConnectorRecordEnvelope,
    *,
    namespace_products: dict[str, str],
    product_policies: dict[str, str],
    policy_digests: dict[str, str],
) -> ConnectorRecordEnvelope:
    validate_envelope_against_batch(batch, envelope)
    if (
        namespace_products.get(envelope.source_namespace_id)
        != envelope.source_product_id
    ):
        raise ValueError(
            "record envelope namespace is not registered for its source product"
        )
    if product_policies.get(envelope.source_product_id) != envelope.policy_id:
        raise ValueError(
            "record envelope does not bind the source product rights policy"
        )
    if policy_digests.get(envelope.policy_id) != envelope.policy_digest:
        raise ValueError(
            "record envelope rights policy is absent or changed in the pinned registry"
        )
    return envelope


def _expected_counts(
    records: int,
    mapped: Iterable[MappedAssertions],
) -> dict[str, int]:
    values = list(mapped)
    counts = {table: 0 for table in DATA_TABLE_COLUMNS}
    counts["community_source_record"] = records
    counts["community_field_assertion"] = sum(
        len(item.field_assertions) for item in values
    )
    counts["community_identifier_assertion"] = sum(
        len(item.identifier_assertions) for item in values
    )
    counts["community_relationship_assertion"] = sum(
        len(item.relationship_assertions) for item in values
    )
    counts["community_entity_type_assertion"] = sum(
        len(item.entity_type_assertions) for item in values
    )
    return counts


def _build_run(
    *,
    registry: SourceRegistrySnapshot,
    batch: ConnectorBatchManifest,
    record_set: ConnectorRecordSetManifest,
    expected_counts: dict[str, int],
) -> CommunityIngestRun:
    return build_community_ingest_run(
        run_kind=IngestRunKind.SOURCE_ASSERTIONS,
        source_product_id=batch.source_product_id,
        input_id=record_set.record_set_id,
        policy_id=batch.policy_id,
        policy_digest=batch.policy_digest,
        image_digest=batch.image_digest,
        config_digest=batch.config_digest,
        started_at=record_set.created_at,
        expected_counts=expected_counts,
        input_manifest={
            "batchId": batch.batch_id,
            "batchManifest": {
                "batchId": batch.batch_id,
                "sourceSystemId": batch.source_system_id,
                "sourceProductId": batch.source_product_id,
                "connectorId": batch.connector_id,
                "policyId": batch.policy_id,
                "policyDigest": batch.policy_digest,
                "changeSemantics": batch.change_semantics.value,
                "completeness": batch.completeness.value,
                "deleteCoverage": batch.delete_coverage.value,
                "coverageScopeDigest": batch.coverage_scope_digest,
                "sourceWindow": (
                    None
                    if batch.source_window is None
                    else batch.source_window.model_dump(
                        mode="json",
                        by_alias=True,
                        exclude_none=True,
                    )
                ),
                "watermarkBefore": batch.watermark_before,
                "watermarkAfter": batch.watermark_after,
                "acquiredAt": batch.acquired_at,
                "recordCount": batch.record_count,
            },
            "recordSetId": record_set.record_set_id,
            "registryDigest": registry.digest,
        },
    )


def build_source_silver_rows(
    *,
    registry: SourceRegistrySnapshot,
    batch: ConnectorBatchManifest,
    record_set: ConnectorRecordSetManifest,
    envelopes: Iterable[ConnectorRecordEnvelope],
    max_records: int = 10_000,
) -> tuple[CommunityIngestRun, dict[str, list[dict[str, Any]]]]:
    """Build a bounded local projection for tests and small replay jobs."""

    if max_records < 1:
        raise ValueError("max_records must be positive")
    namespace_products, product_policies, policy_digests = _validate_inputs(
        registry,
        batch,
        record_set,
    )
    records = tuple(
        _validate_envelope_registry(
            batch,
            envelope,
            namespace_products=namespace_products,
            product_policies=product_policies,
            policy_digests=policy_digests,
        )
        for envelope in validate_envelopes_against_batch(batch, envelopes)
    )
    if len(records) > max_records:
        raise ValueError("local source projection exceeds max_records")
    if len(records) != record_set.record_count:
        raise ValueError("records do not match record-set count")
    if records:
        if records[0].envelope_key != record_set.first_envelope_key:
            raise ValueError("first envelope key does not match record set")
        if records[-1].envelope_key != record_set.last_envelope_key:
            raise ValueError("last envelope key does not match record set")
    mapper = mapper_for_product(batch.source_product_id)
    mapped = [mapper(record) for record in records]
    run = _build_run(
        registry=registry,
        batch=batch,
        record_set=record_set,
        expected_counts=_expected_counts(len(records), mapped),
    )
    rows = empty_data_rows()
    rows["community_source_record"] = [
        source_record_row(run.run_id, record) for record in records
    ]
    rows["community_field_assertion"] = [
        field_assertion_row(run.run_id, assertion)
        for item in mapped
        for assertion in item.field_assertions
    ]
    rows["community_identifier_assertion"] = [
        identifier_assertion_row(run.run_id, assertion)
        for item in mapped
        for assertion in item.identifier_assertions
    ]
    rows["community_relationship_assertion"] = [
        relationship_assertion_row(run.run_id, assertion)
        for item in mapped
        for assertion in item.relationship_assertions
    ]
    rows["community_entity_type_assertion"] = [
        entity_type_assertion_row(run.run_id, assertion)
        for item in mapped
        for assertion in item.entity_type_assertions
    ]
    return run, validate_data_rows(run, rows)


def build_source_silver_dataframes(
    spark: Any,
    *,
    registry: SourceRegistrySnapshot,
    batch: ConnectorBatchManifest,
    record_set: ConnectorRecordSetManifest,
    materialized_shards: Sequence[MaterializedRecordShard] | None = None,
) -> tuple[CommunityIngestRun, dict[str, Any]]:
    """Map immutable envelope shards on Spark without any source API access."""

    namespace_products, product_policies, policy_digests = _validate_inputs(
        registry,
        batch,
        record_set,
    )
    if materialized_shards is None:
        materialized_shards = tuple(
            materialized_shard_from_reference(reference)
            for reference in record_set.record_objects
        )
    bind_materialized_shards(materialized_shards, record_set.record_objects)
    validate_stream_envelope_key_bounds(materialized_shards, record_set)
    mapper = mapper_for_product(batch.source_product_id)

    def parse_record(line):
        envelope = ConnectorRecordEnvelope.model_validate_json(line["value"])
        return _validate_envelope_registry(
            batch,
            envelope,
            namespace_products=namespace_products,
            product_policies=product_policies,
            policy_digests=policy_digests,
        )

    if materialized_shards:
        envelopes = (
            spark.read.text([shard.spark_uri for shard in materialized_shards])
            .rdd.map(parse_record)
            .persist()
        )
    else:
        envelopes = spark.sparkContext.emptyRDD().persist()
    mapped = envelopes.map(lambda envelope: (envelope, mapper(envelope))).persist()
    try:
        record_count = envelopes.count()
        if record_count != record_set.record_count:
            raise ValueError("records do not match record-set count")
        duplicate = (
            envelopes.map(lambda item: (item.envelope_key, 1))
            .reduceByKey(lambda left, right: left + right)
            .filter(lambda pair: pair[1] > 1)
            .take(1)
        )
        if duplicate:
            raise ValueError("record set contains duplicate envelope keys")
        validate_stream_envelope_key_bounds(materialized_shards, record_set)
        bind_materialized_shards(materialized_shards, record_set.record_objects)

        expected_counts = {table: 0 for table in DATA_TABLE_COLUMNS}
        expected_counts["community_source_record"] = record_count
        expected_counts["community_field_assertion"] = int(
            mapped.map(lambda pair: len(pair[1].field_assertions)).sum()
        )
        expected_counts["community_identifier_assertion"] = int(
            mapped.map(lambda pair: len(pair[1].identifier_assertions)).sum()
        )
        expected_counts["community_relationship_assertion"] = int(
            mapped.map(lambda pair: len(pair[1].relationship_assertions)).sum()
        )
        expected_counts["community_entity_type_assertion"] = int(
            mapped.map(lambda pair: len(pair[1].entity_type_assertions)).sum()
        )
        run = _build_run(
            registry=registry,
            batch=batch,
            record_set=record_set,
            expected_counts=expected_counts,
        )
        row_rdds = {
            "community_source_record": mapped.map(
                lambda pair: source_record_row(run.run_id, pair[0])
            ),
            "community_field_assertion": mapped.flatMap(
                lambda pair: (
                    field_assertion_row(run.run_id, assertion)
                    for assertion in pair[1].field_assertions
                )
            ),
            "community_identifier_assertion": mapped.flatMap(
                lambda pair: (
                    identifier_assertion_row(run.run_id, assertion)
                    for assertion in pair[1].identifier_assertions
                )
            ),
            "community_relationship_assertion": mapped.flatMap(
                lambda pair: (
                    relationship_assertion_row(run.run_id, assertion)
                    for assertion in pair[1].relationship_assertions
                )
            ),
            "community_entity_type_assertion": mapped.flatMap(
                lambda pair: (
                    entity_type_assertion_row(run.run_id, assertion)
                    for assertion in pair[1].entity_type_assertions
                )
            ),
        }
        dataframes = {}
        try:
            for table in DATA_TABLE_COLUMNS:
                frame = spark.createDataFrame(
                    (
                        row_rdds[table]
                        if table in row_rdds
                        else spark.sparkContext.emptyRDD()
                    ),
                    schema=community_table_schema(table),
                ).persist()
                if frame.count() != expected_counts[table]:
                    frame.unpersist()
                    raise RuntimeError(f"{table} materialized count changed")
                dataframes[table] = frame
            return run, dataframes
        except Exception:
            for frame in dataframes.values():
                frame.unpersist()
            raise
    finally:
        mapped.unpersist()
        envelopes.unpersist()
