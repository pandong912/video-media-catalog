# video-media-catalog

多来源全球影视研究目录的批处理数据工程。Python 工程只负责：

```text
capture -> Silver -> Identity -> Gold -> OpenSearch build
```

项目不提供 HTTP 服务。Iceberg Silver / Gold 与 commit-last 控制对象是数据事实
边界；OpenSearch 是可从 Gold release commit 完整重建的 projection。

## 数据链

1. Capture 将官方 dataset / API 响应发布为 immutable raw `ObjectRef`、
   connector batch manifest 与 record-set manifest。
2. `video-media-catalog-community-spark` 验证并物化 record shards，将来源记录
   映射为 Silver assertions，最后发布 ingest commit。
3. `video-media-catalog-research-silver resolve-identity` 只接受 pinned、已提交的
   source runs，构建 identity ledger、external-ID index、membership、decision
   与 conflict。
4. `publish-snapshot` 发布 bounded Silver schema `2.0` handoff；
   `publish-epoch` 发布可扩展的 schema `3.0` epoch。
5. `video-media-catalog-gold-spark` 构建唯一 `research` Gold release。
6. `video-media-catalog-gold-index` 从 immutable Gold release commit 构建
   versioned OpenSearch index，并在完整计数核对后原子切换
   `media-catalog-research-read`。

Gold OpenSearch mapping 固定使用 projectionVersion `6`。删除旧流水线不会把
Silver / Gold / epoch 的内部契约版本回退或改名。

## Capture entrypoints

### Wikidata

同步官方日期化 dump：

```bash
video-media-catalog-wikidata-sync \
  --source-url \
    https://dumps.wikimedia.org/wikidatawiki/entities/20260901/wikidata-20260901-all.json.bz2 \
  --destination-prefix s3://catalog-input/wikidata/raw \
  --aws-region us-east-1
```

全量影视、父级与 credit closure backfill：

```bash
video-media-catalog-wikidata-full-media \
  --dump-uri s3://catalog-input/wikidata/raw/date=20260901/...json.bz2 \
  --dump-sha256 <hex> --dump-size <bytes> \
  --dump-version <VersionId> --dump-etag <ETag> \
  --staging-prefix s3://catalog-work/wikidata \
  --output-prefix s3://catalog-input/wikidata/full-media \
  --mode backfill --confirm-full-backfill \
  --image-digest sha256:<hex>
```

默认 `--mode profile`，不会发布 connector artifacts。实际 backfill 必须显式
确认。规范化、P31 / P279 closure、父级 closure 和分片均在 Spark executor
执行。

### TVmaze

```bash
video-media-catalog-tvmaze-sync \
  --destination-prefix s3://bucket/research-captures \
  --user-agent 'video-media-catalog/0.1 contact@example.com' \
  --image-digest sha256:<hex>

video-media-catalog-tvmaze-delta-sync \
  --destination-prefix s3://bucket/research-captures \
  --since day \
  --user-agent 'video-media-catalog/0.1 contact@example.com' \
  --image-digest sha256:<hex>
```

Full connector 只访问官方分页 show index。Delta 先捕获 update index，再按 ID
捕获 detail；明确的 detail 404 才生成 DELETE。

### IMDb

```bash
video-media-catalog-imdb-sync \
  --destination-prefix s3://bucket/research-captures \
  --user-agent 'video-media-catalog/0.1 contact@example.com' \
  --acquired-at 2026-09-20T00:00:00Z \
  --dataset-parallelism 7 \
  --image-digest sha256:<hex>
```

一次 snapshot 必须完整包含 IMDb 官方七个 gzip TSV，并通过固定 header 与分区
manifest 校验。

### TMDB

```bash
video-media-catalog-tmdb-sync daily-export \
  --export-date 2026-09-20 \
  --destination-prefix s3://bucket/research-captures \
  --user-agent 'video-media-catalog/0.1 contact@example.com' \
  --image-digest sha256:<hex>

export MEDIA_CATALOG_TMDB_API_READ_TOKEN='<token>'
video-media-catalog-tmdb-sync changes \
  --window-start 2026-09-19 --window-end 2026-09-20 \
  --destination-prefix s3://bucket/research-captures \
  --user-agent 'video-media-catalog/0.1 contact@example.com' \
  --image-digest sha256:<hex>
```

Token 仅从环境变量读取，不进入 URL、manifest、日志或 digest。

### EIDR exact lookup

```bash
video-media-catalog-eidr-backfill extract-ids \
  --silver-snapshot-uri s3://bucket/research-silver/snapshot.json \
  --silver-snapshot-hash sha256:<hex> \
  --silver-snapshot-size <bytes> \
  --silver-snapshot-version <VersionId> \
  --silver-snapshot-etag <ETag> \
  --source-release-id sha256:<hex> \
  --destination-prefix s3://bucket/research-captures \
  --created-at 2026-09-20T00:00:00Z \
  --warehouse s3://bucket/catalog-warehouse
```

该入口只补全 Silver 中已发现的 EIDR ID，不提供 title search、crawl 或未授权的
registry mirror。

### Europeana OAI-PMH

```bash
video-media-catalog-europeana-oai \
  --destination-prefix s3://bucket/landing/research/capture \
  --acquired-at 2026-09-26T00:00:00Z \
  --window-start 2026-09-25T00:00:00Z \
  --window-end 2026-09-25T23:59:59Z \
  --set-spec 9200365 \
  --image-digest sha256:...
```

该入口只访问 Europeana 官方免 key OAI-PMH 端点，固定为 PARTIAL/DELTA，
默认最多 5 页、100 条。它保留每条记录的 `edm:rights`/`dc:rights` 与
RightsStatements/CC URI；preview、音视频 URL 只作为引用事实，绝不下载媒体
二进制。Search/Record API key 不支持命令行传入。完整合规与恢复契约见
`docs/sources/europeana.md` 和 `contracts/europeana_oai_capture.v1.md`。

## Silver

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
  --record-staging-prefix s3://bucket/catalog-warehouse/research/control/record-shards \
  --committed-at 2026-09-20T00:10:00Z \
  --catalog-type glue --catalog-name media \
  --namespace video_media_catalog \
  --warehouse s3://bucket/catalog-warehouse
```

S3 record shards 必须先复制到 catalog warehouse 内 checksum-addressed staging
位置。source / assertion / identity 行只有在 `community_ingest_commit` 写入后
可见。

## Identity orchestration

```bash
video-media-catalog-research-silver publish-snapshot \
  --run-id sha256:<source-run> \
  --snapshot-uri s3://bucket/research-silver/pre-identity.json \
  --created-at 2026-09-20T01:00:00Z \
  --warehouse s3://bucket/catalog-warehouse

video-media-catalog-research-silver resolve-identity \
  --silver-snapshot-uri s3://bucket/research-silver/pre-identity.json \
  --silver-snapshot-hash sha256:<hex> \
  --silver-snapshot-size <bytes> \
  --silver-snapshot-version <VersionId> \
  --silver-snapshot-etag <ETag> \
  --source-run-id sha256:<source-run> \
  --image-digest sha256:<hex> \
  --config-digest sha256:<hex> \
  --started-at 2026-09-20T01:05:00Z \
  --committed-at 2026-09-20T01:15:00Z \
  --warehouse s3://bucket/catalog-warehouse
```

Identity 不读取其他 catalog 或兼容快照。已有 source-run memberships 和
external-ID index 是唯一历史 identity 输入。

现有 Iceberg 物理契约中的 `community_legacy_key_map` 与 ledger
`imported_v1` 列只为避免重写或删除历史云数据而保留；当前 CLI 与 run kind
不会写入 key migration，新的 ledger 行固定 `imported_v1=false`。

人工 curation 继续使用：

```bash
video-media-catalog-identity-curation publish ...
video-media-catalog-identity-curation apply ...
```

## Gold 与 OpenSearch

```bash
video-media-catalog-gold-spark \
  --silver-snapshot-uri s3://bucket/research-silver/epoch.json \
  --silver-snapshot-hash sha256:<hex> \
  --silver-snapshot-size <bytes> \
  --silver-snapshot-version <VersionId> \
  --silver-snapshot-etag <ETag> \
  --silver-snapshot-media-type \
    application/vnd.video-media-catalog.silver-epoch-manifest.v3+json \
  --output-prefix s3://bucket/research-gold \
  --planned-at 2026-09-20T01:30:00Z \
  --committed-at 2026-09-20T01:45:00Z \
  --build-mode release \
  --image-digest sha256:<hex> \
  --warehouse s3://bucket/catalog-warehouse

video-media-catalog-gold-index \
  --release-commit-uri s3://bucket/research-gold/.../release-commit.json \
  --release-commit-hash sha256:<hex> \
  --release-commit-size <bytes> \
  --release-commit-version <VersionId> \
  --release-commit-etag <ETag> \
  --manifest-prefix s3://bucket/gold-index-builds \
  --completed-at 2026-09-20T02:00:00Z \
  --image-digest sha256:<hex> \
  --warehouse s3://bucket/catalog-warehouse \
  --opensearch-endpoint https://search.example.com
```

索引任务使用 AWS 默认凭据链和 SigV4，不接受静态 access key 参数。Full rebuild
是权威路径；可选 affected-entity build 也始终写入新的 concrete index。

## 安装与验证

要求 Python 3.12；Spark 使用 Java 17 与 PySpark 3.5.5。

```bash
uv sync --frozen
uv sync --frozen --extra spark
uv sync --frozen --extra index

make lint
make test
make test-spark
uv build
git diff --check
```

测试不连接真实 AWS 或生产 Spark。完整数据契约见
[`contracts/README.md`](contracts/README.md)，架构边界见
[`docs/architecture/community-catalog-v2.md`](docs/architecture/community-catalog-v2.md)。
