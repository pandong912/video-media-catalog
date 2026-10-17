# Media Catalog contracts

本目录只描述当前批处理数据边界：

- `community_catalog.v2.md`：source registry、rights、capture manifest、
  record envelope、assertion 与 identity 边界；
- `catalog/catalog-source-policy.v1.json`：Platform/Compute/Catalog 共用的
  portable canonical 来源授权策略；Catalog 技术能力由代码内独立映射描述，
  live 权限取两者交集，说明见同目录 `.md`；
- `catalog/global-reference-ontology.v1.json`：七类 CanonicalSubject 根、
  versioned concrete type concepts 及 tenant-neutral authority；
- `catalog/global-reference-release.v1.schema.json`：News/Learning/Music
  reference release、MusicBrainz/Wikidata 双 pin、Gold/index evidence 与零
  World 写入边界；
- `temporal/research-domain-profiles.v1.schema.json`：包含双来源 Music 月度
  workflow 的八个生产 Schedule，以及全部默认关闭的 publication/cutover gate；
- `parquet/community_catalog_silver.v2.md`：Silver run visibility、source /
  assertion / identity Iceberg 表与 commit-last 规则；
- `control/community_silver_epoch.v3.md`：Silver epoch、parent / baseline
  ObjectRef、bounded delta、watermark 与分布式 run count / digest；
- `identity_curation.v2.md`：人工 identity curation manifest、snapshot
  pinning、五类操作与幂等提交；
- `europeana_oai_capture.v1.md`：Europeana 官方 OAI-PMH 的有界
  PARTIAL/DELTA Capture、rights 分离、恢复与 commit-last 边界；
- `parquet/community_catalog_gold.v2.md`：research Gold 六表、quality /
  attribution、release commit 与 OpenSearch projectionVersion `10`。

`schemaVersion` `2.x` 的 Silver / Gold 契约和 `3.0` 的 Silver epoch 保持独立
版本；版本号不会因删除旧流水线而回退。Python 工程不提供 HTTP serving
contract，OpenSearch 是可由 Gold release commit 重建的批处理投影。
