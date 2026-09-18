# Media catalog commit v1

控制对象使用 lowerCamelCase ProtoJSON 字段名；int64/uint64 值编码为十进制
字符串。未知附加字段可被读取端保留。`SnapshotSet.schemaVersion` 与
`OutputCommit.schemaVersion` 均为 `1.0`。

固定值：

- `stage = "media-catalog-commit"`
- `producer = "video-media-catalog-spark/1.0.0"`
- algorithm spec ID = `media-catalog-wikidata-eidr-v1`
- `algorithmDigest = sha256:b0fe12dbe3670909f5a54c416a247b503eb49515a9da7d6d22754017bbb57c89`

`SnapshotSet` 包含 snapshotSetId、computeRunId、jobSpecId、system
`tenantId`、attempt、stage、inputManifest、六张表 metadata、createdAt、
outputCount、producer、algorithmDigest、imageDigest、configDigest 和字符串
metrics。inputManifest 必须原样重建 JobSpec 的 Parquet ObjectRef，包括
checksum、size、ETag、objectVersion；不得替换成 landing JSON。

每个 table metadata 使用 `tableName`、可选 `snapshotId`、可选
`parentSnapshotId`、`committedAt`、`operation`、`recordCount`。六表必须共享
一个非空 namespace prefix。没有 snapshot 的空表使用 `operation=empty`、
`recordCount=0` 并省略 snapshot IDs，禁止使用 0。表内全球事实没有 tenant。

`OutputCommit` 包含 commitId、computeRunId、jobSpecId、system tenantId、
指向 SnapshotSet 的 outputManifest、committedAt、outputCount、
totalDurationUs、producer 和 labels。labels 重复绑定 stage、attempt、
snapshot set ID、algorithm spec/digest、image/config/input manifest digest
及各表计数。`input_manifest_digest` 固定为 `sha256:hex:<hex>`。SnapshotSet
metrics 与 OutputCommit labels 都必须包含 `algorithm_spec_id`、
`algorithm_digest` 和六个 `<table>_count`。

发布顺序固定为：六张 Iceberg 表 MERGE 完成，读取各表 snapshot ID，发布
`snapshot-set.json`，最后发布 `output.commit.json`。读取端只能把最后一个
对象视为可见性边界。对象采用 create-if-absent；重跑必须验证已有
SnapshotSet/OutputCommit 与本次运行完全兼容，否则失败。

run/job/tenant、snapshotSetId、commitId 均是 canonical lowercase UUIDv7。
后两个 ID 使用 run UUIDv7 timestamp 和完整 immutable identity 的 SHA-256
随机位确定性构造。控制对象固定写入
`<outputPrefix>/attempt=<n>/stage=media-catalog-commit/`。
