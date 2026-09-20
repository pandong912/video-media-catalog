# Media Catalog contracts

本目录是 `video-media-catalog` 自有、可版本化的数据边界，不引用相邻仓库。

- `parquet/media_catalog.v1.md`：JobSpec Parquet source manifest、landing
  Parquet、六张 curated Iceberg 表及兼容规则。
- `control/media_catalog_commit.v1.md`：`SnapshotSet` 与 `OutputCommit` 的
  ProtoJSON 子集和 commit-last 规则。
- `control/community_silver_epoch.v3.md`：Silver epoch canonical JSON、
  parent/baseline ObjectRef、bounded delta、watermark 与分布式 run
  count/digest 契约。
- `community_catalog.v2.md`：供应商中立的 rights、source registry、
  connector envelope、SourceWatermark/capture-window receipt、assertion、
  identity ledger 与 policy-specific release 边界，以及
  Wikidata/EIDR/TVmaze/IMDb/TMDB acquisition、条件发布、重放和删除语义。
  V2 仅发布一套 owner-only `research` release；它是并行契约，不改变 v1。
- `parquet/community_catalog_silver.v2.md`：v2 run visibility、source/
  assertion/identity Iceberg 表、commit-last、兼容 v2 snapshot、可扩展 v3
  epoch manifest，以及 dry-run-first Iceberg maintenance 安全规则。
- `identity_curation.v2.md`：人工 identity review/curation manifest、
  snapshot pinning、五类操作、幂等提交和只读 API 边界。
- `parquet/community_catalog_gold.v2.md`：research Gold 实体、字段、
  identifier、relation、conflict、来源/许可 serving projection 与
  release commit-last 规则。
- `reference_catalog_selection.v1.md`：10 万内容实体、独立 agent budget、
  asset demand profile、层级闭包优先和 selection audit 规则。
- `catalog_asset_match.v1.md`：AssetVersion 匹配请求、候选 Manifest、显式确认
  边界，以及 300 条黄金集离线评测门禁。

各控制对象独立版本化；v1 commit 为 `1.0`，community contracts 为 `2.0`，
Silver epoch manifest 为 `3.0`。新增可空字段是兼容变更；删除字段、改变
字段类型、主键输入或既有枚举语义均需要新 major contract。全球目录事实
不含 `tenant_id`。控制面触发运行及最终 commit 必须携带 canonical UUIDv7
system tenant，该值只出现在控制对象中。
