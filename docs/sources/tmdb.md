# TMDB 私有研究采集

本路径只用于个人、非商业研究。它遵循 TMDB API 条款并保留固定署名：

> This product uses the TMDB API but is not endorsed or certified by TMDB.

元数据允许在 `research_private` 区域内存储、转换、搜索和展示。不得据此推断
再分发、公开导出、训练或图片再利用权限；终止 TMDB 授权时需要清理对应数据。

## 官方入口

- Daily ID Exports：`https://files.tmdb.org/p/exports/`
- API v3：`https://api.themoviedb.org/3/`
- API 文档：`https://developer.themoviedb.org/docs/getting-started`
- 使用条款：`https://www.themoviedb.org/api-terms-of-use`

Daily ID Exports 无需认证，提供 movie、TV 和 person 的完整有效 ID inventory
以及少量筛选字段。changes/detail 需要 TMDB 账号签发的 API Read Access Token。
Token 必须手工写入 AWS Secrets Manager
`ai-video-platform/dev/media-catalog/tmdb` 的 JSON 属性 `apiReadToken`，由
External Secrets 同步到 Temporal Worker Secret。不得写入命令行参数、URL、代码、
日志、artifact、Git 或 OpenTofu state。

## 自动化模式

本地/手动入口 `video-media-catalog-tmdb-capture` 仍可组合两个受限 connector：

- `bootstrap` 先提交指定日期的完整 Daily ID Export，再提交最多 14 个自然日的
  changes/detail。每个自然日独立采集，避免跨日分页超过 TMDB API 上限；若单日
  变更 ID 超过单批上限，入口会按确定性 cursor 依次提交全部 bounded shards。
- `daily` 只提交显式 changes/detail 窗口。
- `inventory-only` 只提交完整 Daily ID Export。

生产编排由 Temporal Python Worker 承担（GitOps 部署）：

- K8s namespace：`media-catalog-research`
- Temporal namespace：`vw-media-catalog-research`
- Workflow task queue：`vw-media-catalog-tmdb-v1`
- Source Silver activity queue：`vw-media-catalog-tmdb-silver-v1`（单并发）
- 每日 09:17 UTC schedule 采集前一个 UTC 自然日的 changes/detail，并顺序提交
  Source Silver
- 每月 2 日 10:47 UTC schedule 刷新 inventory + Source Silver
- 首次 bootstrap 固定 inventory `2026-10-07` 与
  `2026-09-24..2026-10-07` 逐日 changes，由幂等管理 Job 启动一次
- GitHub Actions `.github/workflows/tmdb-capture.yml` 仅保留退役说明，不再执行
  生产采集

## 私有对象布局

固定目标根：

```text
s3://ai-video-platform-dev-media-catalog-209479308066/
  landing/research/capture/
```

连接器只在以下内容寻址路径写入对象：

```text
tmdb/daily-exports/<capture-id>/...
tmdb/changes/<capture-id>/...
tmdb/batches/<batch-id>/batch-manifest.json
tmdb/batches/<batch-id>/record-set.json
tmdb/batches/<batch-id>/records/...
```

raw 对象和 record shards 先写入，batch manifest 与 record-set manifest
commit-last 发布。每个 ObjectRef 固定 SHA-256、大小、VersionId 和 ETag；
同路径不同内容会失败，不覆盖已有对象。每个不可变 capture 后由 Temporal
Activity 提交 EMR Serverless Source Silver，并把 pipeline summary 写到
`landing/research/pipeline-summaries/tmdb/`。

## 首次运行

首次 bootstrap 前必须同时满足：

1. Secrets Manager 容器 `ai-video-platform/dev/media-catalog/tmdb` 已由
   OpenTofu 创建，且已手工写入 `apiReadToken`；
2. media-catalog-tmdb IRSA、EMR digest 与 GitOps Worker/schedules 已部署；
3. default branch commit 的普通/EMR 不可变镜像已发布并通过 Critical gate。

Worker Activity 先调用官方 authentication endpoint 验证 Token，再开始下载。
任一步骤失败都不会伪造成功 manifest；已完成的独立 batch 保持不可变，可安全
重试。

## 发布边界

此流水线只采集私有 source batches 并提交 Source Silver，不运行 Identity、
Gold 或 OpenSearch。它不会创建候选索引，也不会修改
`media-catalog-research-read` alias、Video World publication 或任何全局
cutover gate。
