package com.demo.lake.batch.job;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.cli.Args;
import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * 表维护：小文件合并 + manifest 重写 + 过期快照清理 + 孤儿文件清理。
 * 这四件事是 Iceberg 表长期跑下去的必需品，建议按天调度。
 *
 * <p>对象存储的 bucket lifecycle 只能按前缀/时间删，不知道文件是否还被快照引用；
 * remove_orphan_files 才是「引用感知」的清理，和生命周期规则互补（见 docs/STORAGE.md）。</p>
 */
public class MaintainJob implements LakeJob {

    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public String name() {
        return LakeJobs.MAINTAIN;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig cfg, Args args) {
        String catalog = cfg.catalogName();
        String db = cfg.database();
        int retainLast = args.getInt("retain-last", 5);
        int orphanRetentionHours = args.getInt("orphan-retention-hours", 168);

        for (String table : new String[]{LakeTables.BRONZE_ORDERS, LakeTables.SILVER_ORDERS,
                LakeTables.GOLD_DAILY_REVENUE}) {
            System.out.println("[maintain] ===== " + table + " =====");
            printFileStats(spark, TableRef.of(cfg, table));

            // 1) 合并小文件：min-input-files 调小，让本地小数据集也能看到效果
            //    注意：如果每个分区只有 1 个文件，没有"小文件问题"可治，返回 0 是正常的
            Dataset<Row> rewriteResult = spark.sql("CALL " + catalog + ".system.rewrite_data_files("
                    + "table => '" + db + "." + table + "', "
                    + "options => map('min-input-files', '2', 'target-file-size-bytes', '33554432'))");
            rewriteResult.show(false);
            // 存储过程返回的计数列类型是 Integer，别直接 getLong
            Object rewrittenCount = rewriteResult.select("rewritten_data_files_count").head().get(0);
            long rewritten = rewrittenCount == null ? 0L : ((Number) rewrittenCount).longValue();
            if (rewritten == 0 && args.flag("verbose")) {
                System.out.println("  — 没有可合并的文件组（每个分区文件数 < min-input-files=2）。"
                        + "连续多天 append 之后再跑一次就能看到合并效果。");
            }

            // 2) 重写 manifest，控制元数据规模
            spark.sql("CALL " + catalog + ".system.rewrite_manifests(table => '" + db + "." + table + "')")
                    .show(false);

            // 3) 清理过期快照（保留最近 retainLast 个）
            String olderThan = LocalDateTime.now(ZoneOffset.UTC).format(TS);
            spark.sql("CALL " + catalog + ".system.expire_snapshots("
                    + "table => '" + db + "." + table + "', "
                    + "older_than => TIMESTAMP '" + olderThan + "', "
                    + "retain_last => " + retainLast + ")")
                    .show(false);

            // 4) 清孤儿文件：只删「没有被任何有效快照引用」且早于阈值的数据文件。
            //    写入失败留下的临时文件、过期快照遗留文件都靠它收尾（默认 7 天）。
            String orphanBefore = LocalDateTime.now(ZoneOffset.UTC)
                    .minusHours(orphanRetentionHours).format(TS);
            spark.sql("CALL " + catalog + ".system.remove_orphan_files("
                    + "table => '" + db + "." + table + "', "
                    + "older_than => TIMESTAMP '" + orphanBefore + "')")
                    .show(false);

            printFileStats(spark, TableRef.of(cfg, table));
        }
    }

    private void printFileStats(SparkSession spark, TableRef t) {
        spark.sql("SELECT '" + t.table() + "' AS tbl, count(*) AS file_cnt, "
                + "sum(file_size_in_bytes) AS total_bytes, "
                + "cast(avg(file_size_in_bytes) AS bigint) AS avg_file_bytes "
                + "FROM " + t.metadata("files")).show(false);
    }
}
