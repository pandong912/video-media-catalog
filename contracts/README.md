# Media Catalog contracts

本目录是 `video-media-catalog` 自有、可版本化的数据边界，不引用相邻仓库。

- `parquet/media_catalog.v1.md`：JobSpec Parquet source manifest、landing
  Parquet、六张 curated Iceberg 表及兼容规则。
- `control/media_catalog_commit.v1.md`：`SnapshotSet` 与 `OutputCommit` 的
  ProtoJSON 子集和 commit-last 规则。

当前 schema version 为 `1.0`。新增可空字段是兼容变更；删除字段、改变字段
类型、主键输入或既有枚举语义均需要新 major contract。全球目录事实不含
`tenant_id`。控制面触发运行及最终 commit 必须携带 canonical UUIDv7
system tenant，该值只出现在控制对象中。
