# Media Catalog contracts

本目录只描述当前批处理数据边界：

- `community_catalog.v2.md`：source registry、rights、capture manifest、
  record envelope、assertion 与 identity 边界；
- `parquet/community_catalog_silver.v2.md`：Silver run visibility、source /
  assertion / identity Iceberg 表与 commit-last 规则；
- `control/community_silver_epoch.v3.md`：Silver epoch、parent / baseline
  ObjectRef、bounded delta、watermark 与分布式 run count / digest；
- `identity_curation.v2.md`：人工 identity curation manifest、snapshot
  pinning、五类操作与幂等提交；
- `parquet/community_catalog_gold.v2.md`：research Gold 五表、quality /
  attribution、release commit 与 OpenSearch projectionVersion `6`。

`schemaVersion` `2.x` 的 Silver / Gold 契约和 `3.0` 的 Silver epoch 保持独立
版本；版本号不会因删除旧流水线而回退。Python 工程不提供 HTTP serving
contract，OpenSearch 是可由 Gold release commit 重建的批处理投影。
