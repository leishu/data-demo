# 对象存储：元数据清理与 lifecycle 规则

Iceberg 把数据文件写在对象存储里，自己维护「哪些文件还被快照引用」。生产环境要同时做两件事：
表属性让 Iceberg 及时清理旧元数据，bucket lifecycle 作为兜底清理「Iceberg 看不见」的垃圾。

## 1. 表属性（已开）

`LakeTables.tableProperties()` 给每张托管表都设了：

```sql
'write.metadata.delete-after-commit.enabled' = 'true'
'write.metadata.previous-versions-max'       = '20'
```

含义：每次提交后删掉不再需要的旧 metadata json，最多保留最近 20 个版本。
这样 HMS / 文件系统里不会堆成百上千个元数据文件，也省掉 list 开销。
**本地 Hadoop catalog 和对象存储行为一致**，不需要额外配置。

## 2. Bucket lifecycle（对象存储侧）

`infra/storage/lifecycle.json` 提供三条规则：

| 规则 | 作用 | 为什么 |
|---|---|---|
| `abort-incomplete-multipart-uploads` | 7 天后中止未完成的分片上传 | 写入进程崩溃会留下分片，占空间且 Iceberg 不知道 |
| `expire-iceberg-metadata-old-versions` | `warehouse/` 前缀下的非当前版本 30 天后过期 | 配合表属性，兜住被覆盖的 metadata / manifest |
| `expire-staging-uploads` | `staging/` 前缀 7 天后删除 | 临时落地区，不进 Iceberg 表 |

> **不要在 `warehouse/` 前缀上配 `Expiration`（按天删当前对象）**：那会直接删掉 Iceberg 还在引用的数据文件。
> 只能删「非当前版本」和「未完成分片」，当前版本必须交给 Iceberg 管理。

应用（需要 AWS CLI v2，凭据走环境变量）：

```bash
bin/storage-lifecycle.sh --dry-run
LAKE_S3_ENDPOINT=http://localhost:9000 LAKE_S3_BUCKET=lake \
  AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=password \
  bin/storage-lifecycle.sh
# 验证
aws --endpoint-url http://localhost:9000 s3api get-bucket-lifecycle-configuration --bucket lake
```

`NoncurrentVersionExpiration` 只有在 bucket 开了版本控制时才生效；MinIO 本地 demo 没开版本控制时，
这条规则无害但也不会起作用，真正干活的是分片中止和 `remove_orphan_files`。

## 3. 引用感知的孤儿文件清理

lifecycle 不知道某个 Parquet 文件是否还被快照引用，所以 `maintain` 作业里加了
Iceberg 的 `remove_orphan_files`：只删「没有被任何有效快照引用」且早于阈值的文件。

```bash
bin/dev-batch.sh --job maintain --orphan-retention-hours 168   # 默认 7 天
```

顺序很关键：**先 expire_snapshots，再 remove_orphan_files**，否则刚过期的快照引用的文件还不敢删。
`maintain` 里已经是这个顺序。阈值给 7 天而不是 1 天，是为了避开「正在跑的写入 / 时间旅行查询」，
给误操作和跨时区留缓冲。

## 4. 验证清单

```bash
# 1. 表属性确实生效（Spark SQL）
bin/dev-batch.sh --job verify-sql --sql-file sql/adhoc/01_daily_revenue.sql  # 正常查询即说明表可读
# 或直接跑一次维护，看 remove_orphan_files 的输出列
bin/dev-batch.sh --job maintain --orphan-retention-hours 168

# 2. bucket 规则
aws --endpoint-url http://localhost:9000 s3api get-bucket-lifecycle-configuration --bucket lake

# 3. Prometheus 表指标：元数据快照数不再无限增长
curl -u operator:operator http://localhost:8088/actuator/prometheus | grep lake_iceberg_snapshots
```
