# Media Catalog contracts

本目录是 `video-media-catalog` 自有、可版本化的数据边界，不引用相邻仓库。

- `parquet/media_catalog.v1.md`：JobSpec Parquet source manifest、landing
  Parquet、六张 curated Iceberg 表及兼容规则。
- `control/media_catalog_commit.v1.md`：`SnapshotSet` 与 `OutputCommit` 的
  ProtoJSON 子集和 commit-last 规则。
- `community_catalog.v2.md`：供应商中立的 rights、source registry、
  connector envelope、assertion、identity ledger 与 policy-specific release
  边界。它是并行 v2 契约，不改变 v1。
- `parquet/community_catalog_silver.v2.md`：v2 run visibility、source/
  assertion/identity Iceberg 表、commit-last 和分区规则。
- `parquet/community_catalog_gold.v2.md`：policy-specific Gold 实体、字段、
  identifier、relation、conflict 与 release commit-last 规则。

当前 schema version 为 `1.0`。新增可空字段是兼容变更；删除字段、改变字段
类型、主键输入或既有枚举语义均需要新 major contract。全球目录事实不含
`tenant_id`。控制面触发运行及最终 commit 必须携带 canonical UUIDv7
system tenant，该值只出现在控制对象中。
