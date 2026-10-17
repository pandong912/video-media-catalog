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
   versioned OpenSearch concrete index；独立 validation receipt 完成后，alias
   promotion 仍是显式且独立授权的操作。

Gold OpenSearch mapping 固定使用 projectionVersion `10`。该 strict projection
在 Sports/Animation 字段之外，增加 versioned CanonicalSubject type concept、
reference slice、reference candidate/relationship 与能力边界；完整 observation
历史仍以 Gold release 为准。删除旧流水线不会把
Silver / Gold / epoch 的内部契约版本回退或改名。

Catalog Match 的来源 enablement 固定在
`contracts/catalog/catalog-source-policy.v1.json`，其 canonical semantic
digest 为
`sha256:6f41f4786996179cda7912e69a8a1eacd841abca77029d97ce328fcad23ab018`。
Wikidata、MusicBrainz core、EIDR、IMDb、TVmaze、TMDB、OpenFootball、
StatsBomb 与 MADB 均可进入 Identity/Gold。MusicBrainz 仅允许离线 core CC0
dump；tags、genres、ratings、annotations、supplementary/live feed 与 cover art
不在授权或 mapper 边界内；Artist 与 Label 分别使用
`musicbrainz.artist`、`musicbrainz.label`，Wikidata `P966` 提供 Label MBID
crosswalk。Catalog 的
source-product、capture/freshness 与 projection 技术能力保存在独立实现映射，
live 权限始终与 canonical policy 求交，不产生第二个授权 digest。Golden
Build 必须使用显式 `sourceRunStates` 钉住 capture/run、freshness、watermark
与用途；被当前构建选中的 canonical source run 会被送入 snapshot、Identity
与 Gold，且流程不会读取或伪造外部凭据。

## Global Research Catalog reference ontology

`contracts/catalog/global-reference-ontology.v1.json` 定义终态通用
`CanonicalSubject` 根类型：`AGENT`、`PLACE`、`PHYSICAL_ENTITY`、`EVENT`、
`WORK`、`CONCEPT`、`FICTIONAL_ENTITY`。每个 concrete type concept 和
predicate 都有独立版本；其 authority 固定为 `GLOBAL_REFERENCE_ONLY`，
`tenantWorldIdentityAuthority=false`。

News/Journalism、Learning/Instruction 与 Music 经过 immutable Capture、
Silver assertion、Identity、Gold 双时态 observation/conflict/citation，再贡献
到 v10 search projection。Music 同时绑定 MusicBrainz core 与 Wikidata Music
快照，并覆盖 `MUSIC_ARTIST`、`MUSIC_LABEL`、work、recording、release-group
与 release；Artist/Label 都落在通用 `AGENT` 根。对应生产 profile 必须显式使用
`identitySpine=true`、`goldContribution=true`、`materialization=false`；
Gold contribution 不等于 tenant/World materialization。
Global reference release 只能声明 `RESEARCH`、`DISPLAY`、`IDENTITY`，并强制
tenant canonical write、World materialization/publication 与 alias activation
计数全部为零。release 精确绑定 Gold plan/commit、slice source snapshot pins、
ontology/predicate/source-policy digest，以及 validated concrete index 的 build
manifest 和 validation receipt；不接受 activation receipt。
MusicBrainz 离线 core 的范围、排除项与固定 fixture 见
`docs/sources/musicbrainz-core.md`。

本地可复验 ontology、predicate、source-policy 坐标和 pinned fixtures：

```bash
video-media-catalog-global-reference coordinates
video-media-catalog-global-reference validate-fixtures \
  --fixture tests/fixtures/research/wikidata-news-reference.v1.json \
  --fixture tests/fixtures/research/wikidata-learning-reference.v1.json \
  --fixture tests/fixtures/research/musicbrainz-music-reference.v1.json \
  --fixture tests/fixtures/research/wikidata-music-reference.v1.json
```

`build-release --bundle <json>` 生成内容寻址的
`global-reference-release.v1`；历史 release 不被就地修改。完整边界见
`docs/architecture/global-reference-catalog.md`。

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

TMDB Daily ID Exports 不需要凭据；`changes` 与 detail API 使用账号签发的
API Read Access Token：

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

`video-media-catalog-tmdb-capture` 为私有研究采集提供三个自动化模式：

- `bootstrap`：一次完整 ID inventory，加最近 1–14 日 changes/detail；
- `daily`：只采集一个显式 bounded changes/detail 窗口；
- `inventory-only`：只刷新完整 ID inventory。

生产采集由 Temporal Worker（GitOps：`media-catalog-research`）编排每日
changes、每月 inventory 与一次性 bootstrap；每个不可变 capture 后顺序提交
EMR Source Silver。Token 经 Secrets Manager + External Secrets 注入 Worker。
`.github/workflows/tmdb-capture.yml` 已退役生产路径。写入
`s3://ai-video-platform-dev-media-catalog-209479308066/landing/research/capture/tmdb/`。
不运行 Identity、Gold、OpenSearch rebuild 或 alias 切换。完整运行边界见
`docs/sources/tmdb.md`。

### Global Reference 领域调度

`video-media-catalog-research-temporal-worker` 在既有
`vw-media-catalog-research` Temporal namespace 上注册
`ResearchCaptureWorkflow`、`ResearchSilverWorkflow` 与
`ResearchDomainWorkflow`。其中 Capture 步骤只接收已经归档到 S3、同时绑定
SHA-256、大小、ETag 与 ObjectVersion 的 connector manifest；它不会在执行时
读取 mutable branch、latest dump 或未归档的 HTTP 响应。

`video-media-catalog-research-temporal-admin ensure-runtime` 从
`RESEARCH_DOMAIN_PROFILES_FILE` 幂等协调八个生产 Schedule：

- Wikidata Sports / Animation / News / Learning：每月；
- MusicBrainz core + Wikidata Music：同一个 Music Schedule，每月；
- OpenFootball：每日；
- StatsBomb / MADB：每周。

每个 Schedule 固定一个领域和一个或多个来源 profile，执行 Capture → Source Silver →
Golden Build；来源失败受有限重试及 failure budget 约束，长历史通过
continue-as-new 截断。profile Secret 由 GitOps ExternalSecret 从
`ai-video-platform/dev/media-catalog/research-domain-profiles` 注入，必须先由
采集发布方写入新的 immutable capture pin，Schedule 才会处理新快照。

News/Learning/Music profile 必须 `identitySpine=true` 且
`materialization=false`；profile gate 同时固定
`referenceWorldMaterializationAllowed=false`、
`referenceWorldPublicationAllowed=false` 与
`tenantCanonicalWriteAllowed=false`。该链路只发布 Research Gold release。它不会写
`world_sports_publication` / `world_animation_publication`，也不会执行
OpenSearch alias activation；资产身份关联仍由 Platform 的 release-pinned
Catalog Match 通道完成。

镜像发布在 `main` 上同时产出 worker 与 EMR digest，并通过仓库 Secret
`VIDEO_WORLD_INFRA_DISPATCH_TOKEN` 向 `video-world-infra` 发出
`media-catalog-research-image-published` 事件；Infra 端只生成 reviewable
GitOps digest PR，不直接部署。新的 profile 先作为带 ObjectVersion 的 S3
candidate 归档，再由 `Publish research-domain profiles` 工作流完成语义校验并
写入单一 Secrets Manager 容器。

### EIDR exact lookup

```bash
video-media-catalog-eidr-backfill extract-wikidata-p2704 \
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

该入口只选择固定 Silver epoch 中由 `wikidata-json-dump` 的 P2704 产生的
`eidr-content`，并把来源范围写入不可变 manifest。个人研究解析使用低频、有界
profile：

```bash
video-media-catalog-eidr-backfill lookup-public-research \
  --manifest-uri s3://bucket/research-captures/eidr/.../manifest.json \
  --manifest-hash sha256:<hex> \
  --manifest-size <bytes> \
  --manifest-version <VersionId> \
  --manifest-etag <ETag> \
  --authorization-evidence-uri s3://bucket/research-control/eidr/authorization.json \
  --authorization-evidence-hash sha256:<hex> \
  --authorization-evidence-size <bytes> \
  --authorization-evidence-version <VersionId> \
  --authorization-evidence-etag <ETag> \
  --authorization-issued-at 2026-09-20T00:00:00Z \
  --destination-prefix s3://bucket/research-captures \
  --acquired-at 2026-09-20T01:00:00Z \
  --image-digest sha256:<hex>
```

默认每批 25 条、单次最多 250 条、请求间隔至少 1 秒；硬上限为单次 1,000
条。输出中的 `sourceSilverInputs` 可直接交给现有 Source Silver fan-out，后续
Silver epoch / Golden Build 会与 IMDb、TMDB 等来源合并。该路径始终为
`PARTIAL`，不提供 title search、定时 crawl、删除推断或未授权的 Registry
mirror。完整操作和许可边界见 `docs/sources/eidr-public-research.md`。

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
  --identity-generation-id research-2026-09 \
  --identity-mode full \
  --image-digest sha256:<hex> \
  --config-digest sha256:<hex> \
  --started-at 2026-09-20T01:05:00Z \
  --committed-at 2026-09-20T01:15:00Z \
  --warehouse s3://bucket/catalog-warehouse
```

`full` 只读固定共享的 source/assertion 物理表，并要求
`research-2026-09` 对应的全部 Identity 物理表为空；旧固定 Identity 表中即使
已有相同 `entity_key` 也不会参与 MERGE。后续 `incremental` 必须传入同一
generation 的 pinned snapshot/epoch，且其 Identity snapshot IDs 仍须是当前
物理 head；只读写该 generation。generation 与 mode 进入 input manifest 和
run ID；跨 generation 或 stale input 直接失败。

发布供 incremental、curation 或 Gold 使用的 handoff 时显式固定 generation：

```bash
video-media-catalog-research-silver publish-epoch \
  --identity-generation-id research-2026-09 \
  --epoch-uri s3://bucket/research-silver/epoch.json \
  --created-at 2026-09-20T01:20:00Z \
  --warehouse s3://bucket/catalog-warehouse
```

Manifest 同时携带 generation 与完整逻辑表→物理表 mapping。Source/control
物理表名固定共享；Identity 物理表使用安全、稳定、长度有界的 generation
后缀。旧的不含 generation manifest 仍按固定表名读取，且不会解析所谓
“latest generation”。

现有 Iceberg 物理契约中的 `community_legacy_key_map` 与 ledger
`imported_v1` 列只为避免重写或删除历史云数据而保留；当前 CLI 与 run kind
不会写入 key migration，新的 ledger 行固定 `imported_v1=false`。

人工 curation 继续使用：

```bash
video-media-catalog-identity-curation publish ...
video-media-catalog-identity-curation apply ...
```

失败且没有 commit marker 的 Identity run 不得原 run 直接重试。先生成机器可读
回滚计划，复核 refs 后再执行；两次都必须使用同一 generation：

```bash
video-media-catalog-iceberg-maintenance rollback-failed-run \
  --run-id sha256:<failed-run> \
  --identity-generation-id research-2026-09 \
  --planned-at 2026-09-20T01:25:00Z \
  --warehouse s3://bucket/catalog-warehouse

video-media-catalog-iceberg-maintenance rollback-failed-run \
  --run-id sha256:<failed-run> \
  --identity-generation-id research-2026-09 \
  --planned-at 2026-09-20T01:25:00Z \
  --references-reviewed --execute \
  --warehouse s3://bucket/catalog-warehouse
```

修复 generation 隔离上线前产生的旧固定表 run 时，不传
`--identity-generation-id`，而是显式传
`--allow-legacy-parent-inference`。该兼容开关只允许依据 Iceberg 的线性 parent
链恢复缺失的旧 snapshot journal，并会同时把未提交的
`community_ingest_run` manifest 放在最后回滚；generation-aware run 禁止使用
这个开关。

回滚要求无 commit、current head 仍由该 run 拥有、parent journal 连续且没有
受保护 snapshot/ref；按写入逆序恢复，不直接删除 S3。EMR Serverless 提交的
默认 `retryPolicy.maxAttempts` 为 `1`，避免基础设施自动重跑 partial run。

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

Gold external-ID blocking 修复上线时只需发布新的 Gold image，并用原
`--silver-snapshot-uri` 及其相同 hash/size/VersionId/ETag 重跑一次 Gold。
该修复读取 pinned Identity ledger/membership 并在 Gold 内重新投影 registry
blocking slot；无需重跑 Silver 或 Identity，也不得更换 snapshot。提交
EMR Serverless job 时保持 `retryPolicy.maxAttempts=1`；失败的 partial run
先按既有无 commit/unchanged-head 保护流程处理，不对同一次 application
启用基础设施自动重试。成功后再从新的 immutable Gold release commit
执行 full OpenSearch rebuild。

重跑验收要求 `duplicateExternalIdCount=0`。真实的跨实体同 blocking-key
冲突不会生成 identifier 行，而会以
`DUPLICATE_EXTERNAL_IDENTIFIER` 写入 `community_gold_conflict` 并计入
conflict/withheld；namespace/type 无法绑定的 assertion 仅计入 withheld。

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
