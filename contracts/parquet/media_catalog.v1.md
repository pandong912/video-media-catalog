# Media catalog v1 数据契约

## 通用约束

所有 JSON 字段使用 UTF-8 canonical JSON：对象键按 Unicode 顺序排列、无无关
空白、禁止 NaN/Infinity。所有 `*_key` 是带域分隔的 canonical JSON 经
SHA-256 计算所得，格式固定为 `sha256:<64 lowercase hex>`。Parquet 使用
Zstandard，时间源值保持 ISO 8601 字符串，避免降低 Wikidata 精度。

全球事实表不含 tenant。system tenant 仅属于运行授权和控制面 commit。

## JobSpec source manifest Parquet

JobSpec `inputManifest` 指向 immutable Parquet，media type 固定为
`application/vnd.apache.parquet`。schema metadata contract 为
`video-media-catalog.source-manifest`、version 为 `1.0`。字段顺序与类型固定：

- `source STRING NOT NULL`
- `uri STRING NOT NULL`
- `sha256 STRING NOT NULL`
- `size_bytes INT64 NOT NULL`
- `compression STRING NOT NULL`
- `object_version STRING NULL`
- `etag STRING NULL`
- `license STRING NOT NULL`

最多 16 行，且 Wikidata/EIDR 各最多一行。`source` 只允许 `wikidata`、
`eidr`；URI 只允许无 credential/query/fragment 的 `s3://` 或 `file://`。
`sha256` 为 64 位 hex。EIDR v1 必须 plain；Wikidata 支持 plain/gzip/bzip2。
每个源必须显式声明经运行方审核的数据许可或授权标识。

runtime 必须先按 JobSpec ObjectRef 严格验证 manifest checksum、size、VersionId
和 ETag，再对每个源对象验证 checksum/size 及可选 VersionId/ETag。

## landing manifest 与完成标记

提取后的 `landing-manifest.json` 是 landing shard manifest，
`schemaVersion="1.0"`：

- `manifestId`：manifest 身份内容的确定性键。
- `algorithmSpecId`：`media-catalog-wikidata-eidr-v1`。
- `inputManifestDigest`：生产 runtime 固定为 JobSpec 输入的
  `sha256:hex:<hex>`，防止 landing 与另一份 source manifest 混用。
- `sources[]`：原始 source manifest 的 `source`、`uri`、`checksum`、
  `sizeBytes`、`compression`、必填 `license`，以及可选
  `objectVersion`、`etag`。
- `shards[]`：绝对 `uri`、文件 `checksum`、字符串形式的 `sizeBytes` 和
  `recordCount`、首尾 `recordKey`，以及 S3 返回的可选 `objectVersion`/`etag`。
- `recordCount` 与 `sourceCounts`。

runtime 固定写到
`<outputPrefix>/attempt=<n>/stage=media-catalog-extract/`。提取器先完成全部
shard，再不可变发布 manifest。最后发布
`landing-summary.json`；只有 `status="COMPLETE"` 的 summary 才表示完整。
同路径同内容重跑复用对象，同路径不同内容立即失败，不混合两次运行。
Spark 阶段必须在读取 shard 前验证 summary、input manifest digest 及每个
shard 的大小和 SHA-256；S3 shard 还必须验证可用的 VersionId/ETag。

## landing Parquet

契约名 `video-media-catalog.landing`，schema metadata version 为 `1.0`。

- `record_key STRING NOT NULL`：source、source record ID、revision、source
  hash 的确定性键。
- `source STRING NOT NULL`：`wikidata` 或 `eidr`。
- `source_record_id STRING NOT NULL`：QID 或规范大写 EIDR DOI。
- `source_revision STRING NULL`：Wikidata `lastrevid`；EIDR 使用本地 XML 的
  last-modified 值（若存在）。
- `modified STRING NULL`：未损失精度的源修改时间。
- `source_hash STRING NOT NULL`：规范化 source payload 的 SHA-256。
- `payload_json STRING NOT NULL`：完整规范化 payload。

Wikidata payload 保留 labels、descriptions、aliases、sitelinks，以及允许列表
中 claims 的 `id`、`rank`、`mainsnak`、`qualifiers`、
`qualifiers-order`。允许列表至少覆盖 P31/P279/P1476/P577/P2047/P364/P495/
P136/P179/P361/P4908/P57/P58/P161/P162/P272/P344/P1040/P725/P345/
P2704/P4529/P5284/P1545/P155/P156/P1113/P2437。EIDR payload 保留 ID、
referent type、由 SeriesInfo/SeasonInfo/EpisodeInfo/EditInfo 推导的
`recordType`、标题和语言、发行日、时长、国家、层级父关系及 AlternateID。

## curated Iceberg 表

六张表均为 Iceberg format-version 2、insert-only。MERGE 只执行
`WHEN NOT MATCHED THEN INSERT`；确定性主键使相同 landing 重跑幂等。

### catalog_source_record

主键 `record_key`。字段：`record_key`、`source`、`source_record_id`、
可空 `source_revision`、可空 `modified`、`source_hash`、`entity_key`、
`payload_json`。它将纳入目录的源记录绑定到 canonical entity。

### catalog_entity

主键 `entity_key`。字段：`entity_key`、`entity_type`、`canonical_source`、
`canonical_source_id`、`attributes_json`。`entity_type` 当前值为 MOVIE、
TV_SERIES、TV_SEASON、TV_EPISODE、PERSON、ORGANIZATION、UNKNOWN。P31/P279
闭包决定 Wikidata 类型；credit 或父关系可以建立类型明确或 UNKNOWN 的引用
占位实体。

### catalog_name

主键 `name_key`。字段：`name_key`、`entity_key`、`name_type`、`language`、
`value`、`source`、`source_record_id`。完整保留所有 Wikidata label/alias、
P1476 标题和 EIDR 标题；缺失语言使用 `und`。

### catalog_external_identifier

主键 `identifier_key`。字段：`identifier_key`、`entity_key`、`scheme`、
`value`、`source`、`source_record_id`。至少发布 wikidata、eidr、imdb、
douban。跨源 canonical 合并只允许规范 EIDR ID 或 IMDb ID 完全相等；标题、
发行年或编辑距离不参与身份判断。

### catalog_relation

主键 `relation_key`。字段：`relation_key`、`subject_entity_key`、
`relation_type`、`object_entity_key`、可空 `ordinal`、`source`、
`source_record_id`、`attributes_json`。Wikidata statement rank、property、
statement ID、全部 qualifiers 均进入 attributes；P1545 的首个值同时进入
`ordinal`。

### catalog_ingest_error

主键 `error_key`。字段：`error_key`、`source`、`source_record_id`、
`error_code`、`message`、`details_json`。坏 landing、一个 EIDR 精确标识符集合
指向多个 QID、或 EIDR/Wikidata 媒体类型矛盾时写入本表；不会进行模糊兜底。

## 预留：asset_catalog_assertion

后续版本将定义租户资产到全球 catalog entity 的 assertion。它不是全球事实，
预计包含 tenant、asset/version、entity、assertion method、confidence、证据
和撤销语义。v1 不创建该表，也不把 tenant 写入上述六张表。
