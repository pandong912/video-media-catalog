# video-media-catalog

基于 Wikidata JSON dump 与离线 EIDR XML 的全球影视目录。生产流水线分为：

1. `video-media-catalog`：严格控制面 runtime 模式，验证 JobSpec 的 immutable
   Parquet source manifest，从 S3/file 有界物化源对象，发布 landing。
2. `video-media-catalog-spark`：Spark 3.5.5 执行类型闭包、精确合并和六张
   Iceberg 表的 insert-only MERGE，最后发布 SnapshotSet 与 OutputCommit。

全球目录表不含 tenant。`tenantId` 只用于控制面运行和 commit。

## 安装

```bash
uv sync --frozen
uv sync --frozen --extra spark
```

要求 Python 3.12；Spark 使用 Java 17。项目由 hatchling 构建，使用 ruff 与
pytest。

## Source manifest

JobSpec `inputManifest` 必须是 immutable Parquet 对象：

- `format=OBJECT_FORMAT_PARQUET`
- `mediaType=application/vnd.apache.parquet`
- checksum 为 SHA-256 HEX
- 必须提供 `sizeBytes`、`etag`、`objectVersion`

Parquet 每行描述一个源对象，固定字段：

- `source STRING NOT NULL`：`wikidata` 或 `eidr`
- `uri STRING NOT NULL`：`s3://` 或 `file://`
- `sha256 STRING NOT NULL`
- `size_bytes INT64 NOT NULL`
- `compression STRING NOT NULL`：`plain`、`gzip`、`bzip2`
- `license STRING NOT NULL`：运行方确认的数据许可或授权标识
- 可空 `object_version`、`etag`

最多各一行 Wikidata/EIDR。EIDR XML v1 只接受 plain；Wikidata 支持三种压缩。
完整契约见
[`contracts/parquet/media_catalog.v1.md`](contracts/parquet/media_catalog.v1.md)。

## 生产提取 runtime

Argo 直接执行 CLI，不带子命令：

```bash
video-media-catalog \
  --manifest-uri s3://catalog-input/manifests/source-manifest.parquet \
  --manifest-hash sha256:hex:<64位hex> \
  --manifest-version <S3 VersionId> \
  --manifest-etag <ETag> \
  --manifest-size <bytes> \
  --run-id 01a081e8-6420-7000-8000-000000000202 \
  --job-spec-id 01a081e8-6420-7000-8000-000000000203 \
  --tenant-id 01a081e8-6420-7000-8000-000000000204 \
  --attempt 1 \
  --output-prefix s3://catalog-output/runs/01a081e8-6420-7000-8000-000000000202 \
  --executor-image registry.example/catalog@sha256:<64位hex>
```

runtime 严格要求 canonical lowercase UUIDv7 和 digest-pinned executor image。
它先核对 source manifest 的 SHA-256、大小、VersionId、ETag，再逐行核对每个
源对象的 SHA-256/大小以及可选 VersionId/ETag。S3 body 始终关闭；认证使用
boto3 默认凭据链，不接收或记录凭据参数。

输出固定为：

```text
<outputPrefix>/attempt=<n>/stage=media-catalog-extract/
  landing/shard-00000.parquet
  landing/shard-00001.parquet
  landing-manifest.json
  landing-summary.json
```

发布顺序为 shards → manifest → summary。S3 使用 `If-None-Match: *`；并发冲突
会重新流式读取已有对象并核对完整 SHA-256/大小，绝不覆盖不同内容。

### Ephemeral storage

全量 dump 不进入内存。源对象和 landing 在临时磁盘有界 spool，解析器逐行/
逐记录消费。默认限制：

- source manifest：64 MiB、最多 16 行
- 单个源：2 TiB
- ephemeral 预算：4 TiB
- 单个不可变输出：5 GiB（S3 single PUT）
- landing shard：50,000 records

可按 Pod 临时盘调整：

```text
--ephemeral-dir
--max-manifest-bytes
--max-source-bytes
--max-ephemeral-bytes
--max-output-object-bytes
--shard-records
```

启动前按 `manifest + 2 × source sizes` 做保守容量检查，并核对文件系统可用空间。
生产 Argo 的 `ephemeral-storage` request/limit 必须覆盖该估算；真实 Wikidata
全量通常远大于 20 GiB。

## 本地开发提取

保留不访问网络的 `extract` 子命令：

```bash
uv run video-media-catalog extract \
  --wikidata-uri /data/wikidata.json.bz2 \
  --eidr-xml-uri /data/eidr.xml \
  --output-uri file:///data/local-landing \
  --shard-records 50000
```

可额外传 `--wikidata-sha256`、`--eidr-sha256`。本模式只接受本地路径或
`file://`，不会默认搜索 EIDR。

## Spark / Iceberg commit

Spark worker 接收相同标准参数，并要求：

```text
--stage media-catalog-commit
```

landing manifest 默认自动推导为：

```text
<outputPrefix>/attempt=<n>/stage=media-catalog-extract/landing-manifest.json
```

Spark 在读取 Parquet 前会验证 `landing-summary.json` 对 manifest 的绑定、
JobSpec input manifest digest，以及每个 shard 的大小和 SHA-256（S3 同时校验
VersionId/ETag）。本地调试可用 `--landing-manifest-uri` 覆盖。生产 catalog
设置可通过环境：

```text
MEDIA_CATALOG_CATALOG_TYPE=glue
MEDIA_CATALOG_CATALOG_NAME=media
MEDIA_CATALOG_NAMESPACE=video_media_catalog
MEDIA_CATALOG_WAREHOUSE_URI=s3://catalog-warehouse/warehouse
AWS_REGION=us-east-1
```

等价 CLI 参数仍可显式传入：

```bash
video-media-catalog-spark \
  --manifest-uri s3://catalog-input/manifests/source-manifest.parquet \
  --manifest-hash sha256:hex:<64位hex> \
  --manifest-version <S3 VersionId> \
  --manifest-etag <ETag> \
  --manifest-size <bytes> \
  --run-id 01a081e8-6420-7000-8000-000000000202 \
  --job-spec-id 01a081e8-6420-7000-8000-000000000203 \
  --tenant-id 01a081e8-6420-7000-8000-000000000204 \
  --attempt 1 \
  --output-prefix s3://catalog-output/runs/01a081e8-6420-7000-8000-000000000202 \
  --executor-image registry.example/catalog@sha256:<64位hex> \
  --stage media-catalog-commit \
  --catalog-type glue \
  --catalog-name media \
  --namespace video_media_catalog \
  --warehouse s3://catalog-warehouse/warehouse
```

Glue 使用 `org.apache.iceberg.aws.glue.GlueCatalog` 与
`org.apache.iceberg.aws.s3.S3FileIO`。`--s3-endpoint`、
`--s3-path-style-access` 可用于兼容 endpoint；不允许传明文凭据。

最终控制对象固定写入：

```text
<outputPrefix>/attempt=<n>/stage=media-catalog-commit/
  snapshot-set.json
  output.commit.json
```

SnapshotSet 的 `inputManifest` 原样绑定 JobSpec Parquet ObjectRef，而非 landing
JSON。OutputCommit label 使用
`input_manifest_digest=sha256:hex:<hex>`。

六个 `IcebergTableSnapshot` 均发布 `tableName`、可选 `snapshotId`/
`parentSnapshotId`、`committedAt`、`operation`、`recordCount`。无 snapshot
且本阶段零行时使用 `operation=empty` 并省略 snapshot IDs，不伪造 0。

SnapshotSet/OutputCommit ID 使用 run UUIDv7 的 timestamp 加 SHA-256 identity
生成稳定 canonical UUIDv7。现有 commit 重用会完整核对 IDs、input/output
ObjectRef、六表 metadata、metrics 和 labels。

固定常量：

- stage：`media-catalog-commit`
- producer：`video-media-catalog-spark/1.0.0`
- algorithm spec：`media-catalog-wikidata-eidr-v1`
- algorithm digest：
  `sha256:b0fe12dbe3670909f5a54c416a247b503eb49515a9da7d6d22754017bbb57c89`

metrics 与 labels 均绑定 algorithm spec/digest 及六表 count。

## 数据规则

- Wikidata plain/gzip/bzip2 one-entity-per-line 有界读取，处理数组首尾及行尾逗号。
- 保存 revision、modified、多语言 labels/descriptions/aliases/sitelinks，以及
  允许 claims 的 rank、mainsnak、qualifiers（含 P1545）。
- P31/P279 闭包识别 MOVIE、TV_SERIES、TV_SEASON、TV_EPISODE。
- credit 引用建立 PERSON/ORGANIZATION。
- EIDR parser namespace-tolerant；SeriesInfo/SeasonInfo/EpisodeInfo/EditInfo
  派生 `recordType`，优先于经常仅为 `TV` 的 ReferentType。
- 跨源只按 EIDR ID 或 IMDb ID 完全相等合并；冲突写 ingest error。
- 全球事实与关系使用 canonical JSON + SHA-256 确定性键。

## Docker 与发布

Docker 固定 Python 3.12、Java 17、Spark/PySpark 3.5.5、Iceberg 1.8.1，
包含 Iceberg AWS bundle、Hadoop AWS 和 AWS SDK bundle 1.12.780。下载对象均
校验固定摘要。

镜像使用 Spark 官方 Kubernetes `/opt/entrypoint.sh`，并内置
`/opt/video-media-catalog/stage.py` 供 SparkApplication 启动 driver/executor。
Argo 提取节点通过 container `command` 显式选择 `video-media-catalog`；直接运行
镜像时默认 CMD 显示 Spark CLI help。

GitHub publish 使用 OIDC 与 immutable ECR digest，需要
`AWS_MEDIA_CATALOG_CI_ROLE_ARN` 和可选 `AWS_REGION`，不保存静态 AWS key。

## 测试

```bash
make verify
make test-iceberg
uv build
git diff --check
```

tests 覆盖 source manifest、S3 metadata/大小/关闭 body/条件写复验、runtime
路径、UUIDv7、控制仓 fixture、EIDR 真实 TV Episode 结构、Spark 闭包和本地
Iceberg commit-last。

## 非目标与许可

- 不下载真实 Wikidata dump，不提供 EIDR 默认网络 client。
- 不做标题模糊合并、租户资产 assertion、搜索 API 或 UI。
- v1 curated 表 insert-only，不执行删除或历史覆盖。

代码使用 Apache License 2.0。Wikidata 通常为 CC0；EIDR metadata 权利取决于
运行方授权，代码许可不授予输入数据权利。
