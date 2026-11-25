# 并发写：Iceberg 的乐观锁与验证

Spark / Trino（甚至 Flink）同时写同一张 Iceberg 表时，靠的是 **catalog 层的乐观并发提交**：
写入者在自己的快照上准备数据文件，提交时把元数据从「当前版本」原子换成新版本；
如果两个写入者基于同一版本提交，后提交者会拿到 `CommitFailedException`，
refresh 后重试即可。不会出现两个写入者都报成功、却只落了一半数据的静默丢失。

## 本地自动化验证（Hadoop catalog）

`spark-batch/src/test/java/com/demo/lake/batch/IcebergConcurrentWriteTest.java`：

1. 建一张 `iceberg.<db>.concurrent_target`，先写一行 base；
2. 用两个独立的 SparkSession 同时 `INSERT` 一行；
3. 断言：
   - 至少一个写入者成功；
   - 若恰好一个成功，另一个必须是 `CommitFailedException`（而不是别的错误）；
   - 最终行数 = base + 成功写入数。

这个测试在 `mvn verify` 里自动跑，CI 也会执行。它验证的是 Iceberg 1.11.0 + Spark 4.1.4
在 Hadoop catalog 下的提交语义。注意 Iceberg 内部有重试（日志里能看到
`Retrying task after failure`），所以「两个都成功」也是合法结果，测试允许。

> 读计数前要 `refreshTable` 并用新会话：Iceberg 的 `HadoopTableOperations` 会缓存当前 metadata，
> 老会话可能还看到写入前的快照。

## HMS catalog 为什么也成立

HMS catalog 把「当前 metadata 文件位置」当作表的版本指针。提交时 Iceberg 读取 HMS 里的表对象，
比较版本后通过 `alter_table` 原子更新位置；版本不匹配同样抛 `CommitFailedException`。
所以 HMS + Iceberg 1.11 与本地 Hadoop catalog 的并发语义一致，区别只是「版本指针」存在 HMS 而不是文件系统。

需要注意的边界：

- 并发 **schema 演进**（`ADD COLUMNS`）与数据写入同时进行时，建议串行：schema 变更先做完再放写任务。
- `rewrite_data_files` / `expire_snapshots` 是表维护，本身也走乐观提交，但和写入并发会放大重试次数；
  这也是 `maintain` 要单独调度的原因之一。
- Flink 侧用 Iceberg Sink 时开 `write.distribution-mode=hash` 并保证同一个 checkpoint 周期只有
  一个提交者；跨引擎提交冲突同样由 catalog 的乐观锁兜底。

## 集群上手动确认（HMS + MinIO）

在已有 infra 的环境里跑两次并发 `ingest`，看两次运行都终态成功、行数正确：

```bash
# 1. 起 infra（HMS + MinIO + Trino）
bin/infra.sh up trino

# 2. 两次写入同时打同一张表；用不同的 run-id 便于在审计里区分
bin/dev-batch.sh --job ingest --orders-per-day 200 --run-id conc-a &
bin/dev-batch.sh --job ingest --orders-per-day 200 --run-id conc-b &
wait

# 3. 看两次运行记录与表行数
bin/trino-query.sh --sql "select count(*) from iceberg.lake.bronze_orders"
curl -u operator:operator 'http://localhost:8088/api/batch/runs?limit=5' | python3 -m json.tool
```

若其中一次报 `CommitFailedException`，说明乐观锁按预期拒绝了陈旧提交；重跑该作业即可。
若两次都成功，行数应是「base + 两次写入量」，而不是只多一次。
