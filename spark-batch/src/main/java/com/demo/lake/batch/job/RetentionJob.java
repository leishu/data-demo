package com.demo.lake.batch.job;

import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.batch.RunIds;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.governance.AssetDefinition;
import com.demo.lake.common.governance.GovernanceCatalog;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 按资产目录登记的保留期删除旧分区；默认 dry-run 只列出待删分区。
 *
 * <p>不碰业务表结构，只用普通 DELETE 按分区键过滤。执行前后都往 {@code ops.retention_audit}
 * 写审计事件，dry-run 也留痕，便于回答「谁在什么时候评估过哪些分区」。</p>
 */
public class RetentionJob implements LakeJob {

    /** 分区表达式：bronze/silver 用 event_time 的日期，gold 用 dt。都是普通 SQL，不依赖隐藏分区函数 */
    private static final Map<String, String> PARTITION = Map.of(
            LakeTables.BRONZE_ORDERS, "CAST(event_time AS DATE)",
            LakeTables.SILVER_ORDERS, "CAST(event_time AS DATE)",
            LakeTables.GOLD_DAILY_REVENUE, "dt");

    @Override
    public String name() {
        return LakeJobs.RETENTION;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig platformConfig, Args args) {
        boolean dryRun = !args.flag("execute");
        String runId = RunIds.resolve(args);
        TableRef target = TableRef.of(platformConfig.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.RETENTION_AUDIT);
        if (!spark.catalog().tableExists(target.fqn())) {
            throw new IllegalStateException("保留审计表不存在，先跑一次 init: " + target.fqn());
        }

        List<Row> audit = new ArrayList<>();
        int totalPartitions = 0;
        long totalRows = 0;
        for (AssetDefinition asset : GovernanceCatalog.ASSETS) {
            String table = asset.getTable();
            String partition = PARTITION.get(table);
            if (partition == null || !spark.catalog().tableExists(TableRef.of(platformConfig, table).fqn())) {
                continue;
            }
            int days = args.getInt("retention-days", asset.getRetentionDays());
            String fqn = TableRef.of(platformConfig, table).fqn();
            String predicate = partition + " < date_sub(current_date(), " + days + ")";

            audit.add(auditRow(runId, "retention.start", table, dryRun, 0, 0,
                    "retention=" + days + "d dry_run=" + dryRun, System.currentTimeMillis()));
            auditLine("retention.start", runId, table, dryRun, 0, 0);

            List<Row> partitions = spark.sql("SELECT CAST(" + partition + " AS STRING) AS part, count(*) AS rows "
                    + "FROM " + fqn + " WHERE " + predicate + " GROUP BY " + partition + " ORDER BY " + partition)
                    .collectAsList();
            int count = partitions.size();
            long rows = 0;
            List<String> names = new ArrayList<>();
            for (Row row : partitions) {
                names.add(row.getString(0) + "(" + row.getLong(1) + ")");
                rows += row.getLong(1);
            }
            String detail = String.join(", ", names);
            System.out.printf("[retention] %s table=%s retention=%dd partitions=%d rows=%d => %s%n",
                    dryRun ? "DRY-RUN" : "EXECUTE", table, days, count, rows,
                    detail.isEmpty() ? "-" : detail);

            if (!dryRun && !partitions.isEmpty()) {
                spark.sql("DELETE FROM " + fqn + " WHERE " + predicate);
                System.out.printf("[retention] 已删除 %s 的 %d 个旧分区（%d 行）%n", table, count, rows);
            }
            totalPartitions += count;
            totalRows += rows;
            audit.add(auditRow(runId, "retention.done", table, dryRun, count, rows,
                    detail, System.currentTimeMillis()));
            auditLine("retention.done", runId, table, dryRun, count, rows);
        }

        writeAudit(spark, target, audit);
        System.out.printf("[retention] %s 完成（run_id=%s，待删分区 %d 个 / %d 行，审计事件 %d 条）%n",
                dryRun ? "DRY-RUN" : "EXECUTE", runId, totalPartitions, totalRows, audit.size());
    }

    private static Row auditRow(String runId, String event, String table, boolean dryRun,
                                int partitions, long rows, String detail, long recordedAt) {
        return RowFactory.create(runId, event, table, dryRun, partitions, rows, recordedAt, detail);
    }

    /** 结构化审计行：和 lake-service 的 AuditService 同款 key=value，便于被日志采集系统解析 */
    private static void auditLine(String event, String runId, String table, boolean dryRun,
                                  int partitions, long rows) {
        System.out.printf("AUDIT event=%s run_id=%s table=%s dry_run=%s partitions=%d rows=%d%n",
                event, runId, table, dryRun, partitions, rows);
    }

    private void writeAudit(SparkSession spark, TableRef target, List<Row> rows) {
        if (rows.isEmpty()) {
            return;
        }
        StructType schema = new StructType()
                .add("run_id", DataTypes.StringType, false)
                .add("event", DataTypes.StringType, false)
                .add("table_name", DataTypes.StringType, false)
                .add("dry_run", DataTypes.BooleanType, false)
                .add("partitions", DataTypes.IntegerType, false)
                .add("row_count", DataTypes.LongType, false)
                .add("audited_ms", DataTypes.LongType, false)
                .add("detail", DataTypes.StringType, true);
        try {
            spark.createDataFrame(rows, schema).writeTo(target.fqn()).append();
        } catch (Exception e) {
            // 审计是合规证据，写不进去就让作业失败；静默放过会让「dry-run 评估过」变成无凭无据
            throw new IllegalStateException("写入保留审计事件失败: " + target.fqn(), e);
        }
    }
}
