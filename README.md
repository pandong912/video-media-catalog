# video-media-catalog

基于 Wikidata JSON dump 与离线 EIDR XML 的全球影视目录。生产流水线分为：

1. `video-media-catalog`：严格控制面 runtime 模式，验证 JobSpec 的 immutable
   Parquet source manifest，从 S3/file 有界物化源对象，发布 landing。
2. `video-media-catalog-validate`：Spark 3.5.5 对完整 landing 执行转换和
   分布式质量门禁，只发布质量报告，不写 Iceberg。
3. `video-media-catalog-spark`：Spark 3.5.5 执行类型闭包、精确合并和六张
   Iceberg 表的 insert-only MERGE，最后发布 SnapshotSet 与 OutputCommit。
4. `video-media-catalog-index`：从 SnapshotSet 锁定的六表 Iceberg snapshot
   构建 versioned OpenSearch 索引，校验后原子切换只读 alias。
5. `video-media-catalog-api`：独立、只读且 OIDC fail-closed 的 FastAPI 服务。

全球目录表不含 tenant。`tenantId` 只用于控制面运行和 commit。
Iceberg 六表始终是事实源；OpenSearch 仅是可以从 snapshot 完整重建的查询投影。

## Community catalog v2 foundation

方案 2 以公共/社区数据为主，但不把不同许可的数据混成一张“开放”表。当前分支
新增供应商中立的 v2 基础契约：

- `source_registry.py`：区分 source system、product、ID namespace、native
  schema 与 rights profile；
- `rights.py`：按用途、受众、地域、期限和物理 policy zone 做 fail-closed
  权利判断；
- `connector.py`：统一 full/delta/leased、coverage、watermark、delete
  semantics、原始 ObjectRef 与 record envelope；
- `assertions.py` / `identity_v2.py`：事实断言与永久内部实体键分离，保留
  evidence、可逆 decision、membership、redirect 和全部 v1 key；
- `attribution.py`：为 CC BY/BY-SA 发布生成确定性、可审计的来源与许可清单；
- `community_release.py`：发布 policy-specific Gold，区分 affected count 与
  snapshot total，并绑定 exact input/policy/quality identity。

现有 v1 pipeline、六表、算法摘要和 API 不变。完整设计与边界见
[`docs/architecture/community-catalog-v2.md`](docs/architecture/community-catalog-v2.md)
和
[`contracts/community_catalog.v2.md`](contracts/community_catalog.v2.md)。

TVmaze 是首个 v2 社区 connector。它只访问固定的官方 `/shows?page=N`，
遵守 429/`Retry-After` 和至少 20 calls/10 seconds 的公开限制，先不可变发布
原始 page，再发布 batch manifest、record shards 和最终 record-set marker：

```bash
video-media-catalog-tvmaze-sync \
  --destination-prefix file:///absolute/path/to/community-captures \
  --user-agent 'video-media-catalog/0.1 contact@example.com' \
  --image-digest sha256:<64位hex>
```

生产使用 S3 prefix 时继续通过 AWS 默认凭据链，不接受静态 access key 参数。
TVmaze 元数据进入 `open_sharealike`；mapper 首版故意不提升 image URL，图片必须
经过逐资产权利审核。该 connector 不做标题模糊归并，只输出 source-owned
assertions。

同步结果中的 immutable batch/record-set ObjectRef 可提交到独立的 Silver v2
Spark 入口：

```bash
video-media-catalog-community-spark \
  --batch-manifest-uri s3://bucket/.../batch-manifest.json \
  --batch-manifest-hash sha256:<hex> \
  --batch-manifest-size <bytes> \
  --batch-manifest-version <VersionId> \
  --batch-manifest-etag <ETag> \
  --record-set-manifest-uri s3://bucket/.../record-set.json \
  --record-set-manifest-hash sha256:<hex> \
  --record-set-manifest-size <bytes> \
  --record-set-manifest-version <VersionId> \
  --record-set-manifest-etag <ETag> \
  --committed-at 2026-09-19T00:00:00Z \
  --catalog-type glue \
  --catalog-name media \
  --namespace community_catalog_v2 \
  --warehouse s3://bucket/community-warehouse
```

Silver 表全部带确定性 `run_id`。source/assertion/identity 行只有在
`community_ingest_commit` 最后写入后才可见；失败运行留下的 staged rows 不会进入
Gold。`v1_migration.py` 从 snapshot-pinned 六表导入全部既有 key，原样保存
`entity_key`，不会按新规则重新计算。

Gold v2 继续按 policy context 隔离：

- `identity_resolution.py`：精确匹配只接受唯一候选，未匹配 source node 分配一次
  内部 UUIDv7，歧义进入 conflict，membership/redirect 可按 as-of 重放；
- `identity_spark.py`：分布式读取 active type/identifier assertions，优先与 v1
  external identifiers 做类型兼容的精确连接，未匹配项再分配内部实体；多候选
  直接阻断而不是猜测；
- `gold.py` / `gold_resolution.py`：rights eligibility 先于字段选择；
  `SINGLE` 冲突不任意选供应商，`SET_UNION` 保留多值及 assertion lineage；
- `gold_spark_transform.py`：分布式 join active membership、rights/TTL、source
  provenance，生成 entity/field/identifier/relation/conflict 五类 Gold frames；
- `gold_quality.py`：冲突率、未解析身份率和 rights gate 形成不可变报告；
- `gold_iceberg.py`：entity/field/identifier/relation/conflict 五表按 release plan
  隔离，质量 PASS 后才发布 commit marker；
- attribution 与 quality ObjectRef 必须绑定实际 payload，才能进入 release commit。

完整表契约见
[`contracts/parquet/community_catalog_gold.v2.md`](contracts/parquet/community_catalog_gold.v2.md)。
`video-media-catalog-gold-spark` 验证 immutable
`CommunitySilverSnapshotSet`，按列出的 committed runs time-travel Silver，
发布 quality/attribution 对象并 commit Gold。v2 shadow OpenSearch 使用独立
alias，不替换 v1：

```bash
video-media-catalog-gold-index \
  --release-commit-uri s3://bucket/.../release-commit.json \
  --release-commit-hash sha256:<hex> \
  --release-commit-size <bytes> \
  --release-commit-version <VersionId> \
  --release-commit-etag <ETag> \
  --manifest-prefix s3://bucket/gold-index-builds \
  --completed-at 2026-09-19T00:00:00Z \
  --image-digest sha256:<hex> \
  --catalog-type glue \
  --warehouse s3://bucket/community-warehouse \
  --opensearch-endpoint https://search.example.com
```

固定 shadow alias 为 `media-catalog-community-v2-shadow-read`。索引文档只包含
有界 titles/identifiers/attributes/relation summary 和 conflict 标记；完整
assertions 与 relations 仍留在 Iceberg。v2 查询 API 属于后续切片。

## 安装

```bash
uv sync --frozen
uv sync --frozen --extra spark
uv sync --frozen --extra index
uv sync --frozen --extra api
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

## 官方 Wikidata dump 同步

`video-media-catalog-wikidata-sync` 只接受官方带日期的 canonical URL：

```bash
video-media-catalog-wikidata-sync \
  --source-url \
    https://dumps.wikimedia.org/wikidatawiki/entities/20260901/wikidata-20260901-all.json.bz2 \
  --destination-prefix s3://catalog-input/wikidata/raw \
  --aws-region us-east-1
```

CLI 拒绝 `latest`、非 HTTPS、非 `dumps.wikimedia.org` host、userinfo、端口、
query、fragment 和越出 allowlist 的重定向。它先读取同目录官方 SHA-1 校验
清单 `wikidata-YYYYMMDD-sha1sums.txt`，再以严格 HEAD 固定大小和
ETag/Last-Modified，并按 `--upload-part-bytes` 顺序执行 HTTP Range GET。每段
完整读完后才计入 SHA-1/SHA-256 并上传，网络读错误、短读、HTTP 408/429/5xx
只重试当前段；`--range-attempts` 默认 5，指数退避可用
`--retry-initial-backoff-seconds` 和 `--retry-max-backoff-seconds` 调整。
完整 SHA-1 核对成功后，使用 server-side multipart copy 条件发布按日期和上游
SHA-1 寻址的最终对象。

同步不会把完整 dump 落盘或读入内存，内存约为一个 upload part，默认硬上限
200 GiB；任何下载、摘要或 S3 错误都会 abort 活跃 multipart upload。当前不支持
跨 Pod 保留或恢复 multipart 状态；Pod 失败会安全 abort，随后由 Workflow
重跑整个同步。

目标 bucket 必须启用 S3 Versioning。最终对象 metadata 绑定 source URL、
上游 SHA-1、日期和 SHA-256；同身份同内容可复用，metadata 或内容冲突会失败。
认证仅使用 boto3 默认凭据链，CLI 不接受 access key/secret 参数。

## 确定性 Wikidata 子集

Spark 3.5.5 CLI 从上述不可变对象构建预算严格的子集：

```bash
video-media-catalog-wikidata-subset \
  --dump-uri \
    s3://catalog-input/wikidata/raw/date=20260901/sha1=<sha1>/wikidata-20260901-all.json.bz2 \
  --dump-sha256 <64位hex> \
  --dump-size <bytes> \
  --dump-version <S3 VersionId> \
  --dump-etag <ETag> \
  --staging-prefix s3://catalog-work/wikidata-normalized \
  --output-prefix s3://catalog-input/wikidata/subsets \
  --aws-region us-east-1
```

默认 `target-count=100000`。作品基础预算为 MOVIE 30000、TV_SERIES 15000、
TV_SEASON 10000、TV_EPISODE 25000，可分别用 `--movie-count`、
`--tv-series-count`、`--tv-season-count`、`--tv-episode-count` 调整。
每类先按 Wikipedia sitelink 数降序、QID 数字升序选择；配额不足时在作品间
确定性回填。剩余预算依次给已选作品的层级目标、按引用频率排序的
PERSON/ORGANIZATION credit 目标，最后从其余可分类实体确定性回填。

全量规范化和 P31/P279 闭包均在 Spark executor 上执行。规范化 Parquet staging
以 dump 完整 ObjectRef 和 normalization algorithm identity 寻址，并以 commit
marker 控制复用；driver 最多 collect/broadcast `target-count` 个 QID。输出加入
所选实体分类所需的 class dependency rows（不计 entity budget），并删除所有
指向未选 QID 的 relation statements。

最终对象是按 QID 排序、one-entity-per-line 的 bzip2，硬上限 4 GiB，可由现有
`iter_wikidata_entities` 和 runtime extract 直接读取。Spark 先写单个临时 part，
driver 再有界流式复制并计算 SHA-256。发布顺序固定为 subset →
`source-manifest.parquet` → `audit-manifest.json`；最后一个 audit manifest 是
commit marker，记录 dump 完整 ObjectRef、配置 digest、六类 selected counts、
dependency rows 和被裁剪 relation statement 数。所有最终 S3 写入都禁止覆盖
冲突内容，并要求完整 ETag 和 VersionId。

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

## Spark 质量门禁

独立 validate stage 使用与 commit stage 完全相同的 LandingManifest、
LandingSummary、每个 shard ObjectRef 和实际 record count 校验，再复用
`transform_landing` 生成六表 DataFrame；它不会创建或写入任何 Iceberg 表：

```bash
video-media-catalog-validate \
  --manifest-uri s3://catalog-input/manifests/source-manifest.parquet \
  --manifest-hash sha256:hex:<64位hex> \
  --manifest-version <S3 VersionId> \
  --manifest-etag <ETag> \
  --manifest-size <bytes> \
  --run-id <UUIDv7> \
  --job-spec-id <UUIDv7> \
  --tenant-id <UUIDv7> \
  --attempt 1 \
  --output-prefix s3://catalog-output/runs/<run-id> \
  --executor-image registry.example/catalog@sha256:<64位hex> \
  --stage media-catalog-validate \
  --expected-entity-count 100000 \
  --entity-count-tolerance-percent 5 \
  --minimum-name-coverage 0.95
```

指标全部通过 Spark 聚合、groupBy 和 anti-join 分布式计算，driver 只接收计数：
六表行数、六表主键 null/duplicate、ingest error 数、relation 两端悬空数、
至少一个名称的 entity 覆盖率，以及 expected entity count 容差。默认 expected
为 0（跳过数量范围）、容差 5%、最低名称覆盖率 0。

无论 PASS/FAILED，stage 都先不可变发布 `quality-report.json`，再以
`quality-summary.json` commit-last。summary 绑定 report 完整 ObjectRef、
RuntimeArguments input identity、landing manifest ID/digest 和 quality config
digest。FAILED 完成发布后 CLI 返回非零。

## Spark / Iceberg commit

Spark worker 接收相同标准参数，并要求：

```text
--stage media-catalog-commit
```

commit 默认要求同 run/attempt 的 validate summary 和 report 均存在、完整
ObjectRef 校验通过、状态为 PASS，且 runtime、landing 和 quality config 互相
绑定；检查发生在任何 `create_tables`/MERGE 之前。测试或旧流程必须显式传
`--no-quality-report-required` 才能关闭，不能依赖 Argo DAG 顺序绕过。

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

## OpenSearch 可重建投影

索引任务只接受已发布的 `snapshot-set.json`，并通过 Iceberg
`snapshot-id` time travel 读取六表，不读取不受约束的“最新”数据。没有 snapshot
的零行表按空表读取。示例：

```bash
video-media-catalog-index \
  --snapshot-set-uri \
    s3://catalog-output/runs/<run>/attempt=1/stage=media-catalog-commit/snapshot-set.json \
  --snapshot-set-hash sha256:hex:<64位hex> \
  --snapshot-set-version <S3 VersionId> \
  --snapshot-set-etag <ETag> \
  --snapshot-set-size <bytes，最大16MiB> \
  --manifest-prefix s3://catalog-control/index-builds \
  --catalog-type glue \
  --catalog-name media \
  --namespace video_media_catalog \
  --warehouse s3://catalog-warehouse/warehouse \
  --opensearch-endpoint https://search-catalog.us-east-1.es.amazonaws.com \
  --aws-region us-east-1
```

其余配置也可使用对应环境变量：

```text
MEDIA_CATALOG_INDEX_MANIFEST_PREFIX
MEDIA_CATALOG_CATALOG_TYPE
MEDIA_CATALOG_CATALOG_NAME
MEDIA_CATALOG_NAMESPACE
MEDIA_CATALOG_WAREHOUSE_URI
MEDIA_CATALOG_OPENSEARCH_ENDPOINT
MEDIA_CATALOG_OPENSEARCH_SERVICE=es
MEDIA_CATALOG_READ_ALIAS=media-catalog-entities-read
AWS_REGION
```

五个 `--snapshot-set-*` 参数全部必填，并与 GitOps WorkflowTemplate 完全一致。
hash 同时接受 `sha256:hex:<hex>` 和 `sha256:<hex>`。索引器构造固定 JSON
`ObjectRef`，先通过 `BoundedObjectStore` HEAD 验证 SHA-256 metadata、大小、
VersionId 和 ETag，再按同一 VersionId 有界下载并核对实际内容；超过 16 MiB
或任何字段不一致都在解析 SnapshotSet 前失败。

任务使用 `opensearch-py` 的 SigV4 signer 和 AWS 默认凭据链，不接受静态 key
参数。Spark executor 按 partition 流式 bulk；driver 只收集每个 partition
的计数摘要，不收集实体文档。文档 `_id` 固定为 `entityKey`。

投影包含展示名及语言、全部名称、描述、sitelink、核心 attributes、外部 ID、
关系与父实体摘要，以及 source record lineage。展示名按
`zh-hans → zh → en → mul → 其他语言` 回退；同语言内优先 PRIMARY、TITLE。
mapping 固定且 `dynamic=strict`。

六表 snapshot identity、mapping/config digest 共同生成安全 build ID 和
versioned index 名。重复触发会复用同一构建。只有 bulk 成功数、失败数及
OpenSearch document count 全部核对通过后，才用一次 alias update 把
`media-catalog-entities-read` 切到新索引；失败时不切 alias，也不删除旧索引。

成功构建会用 S3 `If-None-Match: *` 条件写
`<manifestPrefix>/index-build-<buildId>.json`。manifest 记录六表 snapshot ID、
mapping/config digest、document/error count、index、alias、开始/完成时间，以及
包含 URI、checksum、size、VersionId、ETag 的源 SnapshotSet ObjectRef。
bulk 或 document count 核对失败的尝试会条件写入
`<manifestPrefix>/failed/`，且不会占用可重试的成功 manifest 路径。
该 manifest 仅用于审计与重建，不改变 `media-catalog-commit` 六表及控制对象契约。

## 只读 API

启动命令：

```bash
video-media-catalog-api --host 0.0.0.0 --port 8080
```

生产环境必须配置：

```text
MEDIA_CATALOG_ENVIRONMENT=production
MEDIA_CATALOG_OPENSEARCH_ENDPOINT=https://search-catalog.us-east-1.es.amazonaws.com
MEDIA_CATALOG_OPENSEARCH_SERVICE=es
MEDIA_CATALOG_READ_ALIAS=media-catalog-entities-read
MEDIA_CATALOG_SEARCH_TIMEOUT_SECONDS=5
MEDIA_CATALOG_CURSOR_SECRET=<至少 32 bytes，来自 Secret>
MEDIA_CATALOG_OIDC_ISSUER=https://issuer.example
MEDIA_CATALOG_OIDC_JWKS_URI=https://issuer.example/.well-known/jwks.json
MEDIA_CATALOG_OIDC_AUDIENCE=media-catalog-api
MEDIA_CATALOG_OIDC_REQUIRED_SCOPE=governance.read
AWS_REGION=us-east-1
```

应用也接受 GitOps 的固定未加前缀契约：
`OPENSEARCH_ENDPOINT`、`REGION`、`INDEX_ALIAS`、`OIDC_ISSUER`、
`OIDC_JWKS_URI`、`OIDC_AUDIENCE`、`OIDC_REQUIRED_SCOPE`。
issuer 必须为 HTTPS；JWKS 可为 HTTPS，或仅对 hostname 等于
`svc.cluster.local`/以 `.svc.cluster.local` 结尾的集群服务允许 HTTP。
所有 OIDC URL 都拒绝 credentials、query 和 fragment。

API Pod 必须使用独立 ServiceAccount/IRSA，仅授予读 alias 所需的 OpenSearch
`ESHttpGet`/`ESHttpHead` 权限。搜索和外部 ID 查询固定通过编码安全的
`GET /<alias>/_search` 发送，不需要 POST。除 `/healthz` 外，请求复用同源
`Authorization: Bearer <JWT>`。
服务校验 JWT 签名、`iss`、`aud`、`exp`、非空 `sub`，并要求
`governance.read` scope；不会记录 token。缺少 OIDC 配置时生产服务拒绝启动。
仅测试可同时设置
`MEDIA_CATALOG_ENVIRONMENT=test` 与 `MEDIA_CATALOG_AUTH_DISABLED=true`。

HTTP 契约：

- `GET /healthz`：公开 liveness。
- `GET /api/v1/catalog/search`：`q` 可省略或为空以浏览目录；可选
  `entityType`、`language`、`pageSize`（1–100）、`cursor`。空查询固定使用
  `match_all + filters`。响应顶层为 `items`、`nextCursor`、`totalValue`、
  `totalRelation`；items 是不含 names/relations 的轻量 summary，外部 ID
  最多五条。
- `GET /api/v1/catalog/entities/{entityKey}`：按稳定实体键读取。
- `GET /api/v1/catalog/external-identifiers/{scheme}/{value}`：精确解析并返回
  单个实体；零条为 404，多条为 409。

分页 cursor 是绑定原查询的 HMAC 签名 opaque `search_after`，篡改或跨查询复用
返回 Problem Details。所有查询由固定结构构造，不接受 OpenSearch DSL。
Problem Details 固定包含 `code` 和 `retryable`；401 保留
`WWW-Authenticate: Bearer`。summary description 按请求语言、展示语言、
`zh-hans`、`zh`、`en`、`mul`、首个可用值依次回退。
OpenSearch 超时和 HTTP 超时均有界。FastAPI 生成的 OpenAPI 可由已认证请求从
`/openapi.json` 获取；默认不公开 Swagger/ReDoc。

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
校验固定摘要。批处理镜像继续包含 `video-media-catalog-index` 所需的
`opensearch-py`。

镜像使用 Spark 官方 Kubernetes `/opt/entrypoint.sh`，并内置
`/opt/video-media-catalog/stage.py` 作为 commit 入口，以及独立
`/opt/video-media-catalog/validate_stage.py` 作为 quality gate 入口。
Argo 提取节点通过 container `command` 显式选择 `video-media-catalog`；直接运行
镜像时默认 CMD 显示 Spark CLI help。

`Dockerfile.api` 基于已更新的 Ubuntu Noble，安装 Python 3.12，使用非 root
用户，不包含 Java、Spark 或 PySpark，兼容 read-only root filesystem，并内置
`/healthz` healthcheck。

GitHub publish 使用矩阵分别发布 `video-media-catalog` 和
`video-media-catalog-api`，均使用 OIDC、immutable ECR digest 以及
Critical findings 必须为 0 的门禁。需要
`AWS_MEDIA_CATALOG_CI_ROLE_ARN` 和可选 `AWS_REGION`，不保存静态 AWS key。

## 测试

```bash
make verify
make test-index
make test-api
make test-iceberg
uv build
git diff --check
```

tests 覆盖 source manifest、S3 metadata/大小/关闭 body/条件写复验、runtime
路径、UUIDv7、控制仓 fixture、EIDR 真实 TV Episode 结构、Spark 闭包、本地
Iceberg commit-last、投影/mapping/alias/bulk、cursor/query、OIDC，以及 API
搜索/详情/外部 ID/健康检查。

## 非目标与许可

- 不下载真实 Wikidata dump，不提供 EIDR 默认网络 client。
- 不做标题模糊合并、租户资产 assertion、写 API 或 UI。
- v1 curated 表 insert-only，不执行删除或历史覆盖。

代码使用 Apache License 2.0。Wikidata 通常为 CC0；EIDR metadata 权利取决于
运行方授权，代码许可不授予输入数据权利。
