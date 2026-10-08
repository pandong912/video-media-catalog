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
Token 必须保存为 GitHub Actions Secret
`MEDIA_CATALOG_TMDB_API_READ_TOKEN`，不得写入命令行参数、URL、代码、日志、
artifact 或 manifest。

## 自动化模式

入口 `video-media-catalog-tmdb-capture` 组合现有两个受限 connector：

- `bootstrap` 先提交指定日期的完整 Daily ID Export，再提交最多 14 个自然日的
  changes/detail。每个自然日独立采集，避免跨日分页超过 TMDB API 上限；若单日
  变更 ID 超过单批上限，入口会按确定性 cursor 依次提交全部 bounded shards。
- `daily` 只提交显式 changes/detail 窗口。
- `inventory-only` 只提交完整 Daily ID Export。

GitHub Actions 工作流 `.github/workflows/tmdb-capture.yml`：

- 每日 09:17 UTC 采集前一个 UTC 自然日的 changes/detail；
- 每月 2 日 10:47 UTC 刷新前一个 UTC 自然日的完整 ID inventory；
- 手动运行默认使用 `bootstrap`，日期留空时以昨天为结束日并回填最近 14 日；
- `bootstrap` 在 inventory 成功后按自然日创建 changes jobs，最多并行两个；每个
  job 使用独立的最长 6 小时 OIDC 会话，单日失败不会取消其余日期；
- 同一时间只允许一个采集运行，不会取消已经开始的提交；
- 运行固定为 default branch commit 对应的不可变、已扫描 ECR image digest。

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
同路径不同内容会失败，不覆盖已有对象。工作流只上传不含源数据和 Token 的
capture summary artifact。

## 首次运行

首次手动 `bootstrap` 前必须同时满足：

1. GitHub 仓库 Secret `MEDIA_CATALOG_TMDB_API_READ_TOKEN` 已配置；
2. `AWS_MEDIA_CATALOG_CI_ROLE_ARN` 对上述 `tmdb/` 前缀的最小 S3/KMS 权限已经
   通过 `ai-platform-infra` 的 OpenTofu apply 生效；
3. default branch commit 的 `sha-<commit>` runtime image 已发布并通过
   Critical vulnerability gate。

工作流先调用官方 authentication endpoint 验证 Token，再开始下载。任一步骤
失败都不会伪造成功 manifest；已完成的独立 batch 保持不可变，可安全重试。
工作流分别保留 inventory 与各自然日的无源数据 summary artifacts。

## 发布边界

此工作流只采集私有 source batches，不运行 Silver、Identity、Gold 或
OpenSearch。它不会创建候选索引，也不会修改
`media-catalog-research-read` alias、Video World publication 或任何全局
cutover gate。后续融合必须显式绑定这里生成的 immutable ObjectRef，并单独完成
质量、来源归因和 alias 审批。
