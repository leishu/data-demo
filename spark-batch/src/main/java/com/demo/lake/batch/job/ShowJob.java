package com.demo.lake.batch.job;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.cli.Args;
import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/** 只读巡检：快照、历史、分区、文件布局。Trino 侧也能查同样的元数据表 */
public class ShowJob implements LakeJob {

    /** 运维元数据表也纳入巡检：行数 = 跑过多少次作业 × 常数，方便判断要不要清理 */
    private static final List<String> OPS_TABLES = List.of(
            LakeTables.BATCH_RUNS, LakeTables.DQ_RESULTS, LakeTables.LINEAGE_RUNS, LakeTables.RETENTION_AUDIT);

    private static final String GOLD_SNAPSHOTS_SQL =
            "SELECT snapshot_id, parent_id, operation, "
            + "summary['added-records'] AS added, summary['deleted-records'] AS deleted, "
            + "summary['total-records'] AS total, committed_at "
            + "FROM %s ORDER BY committed_at DESC LIMIT 8";
    private static final String BRONZE_HISTORY_SQL =
            "SELECT made_current_at, snapshot_id, is_current_ancestor "
            + "FROM %s ORDER BY made_current_at DESC LIMIT 8";
    private static final String BRONZE_PARTITIONS_SQL =
            "SELECT partition, record_count, file_count, total_data_file_size_in_bytes "
            + "FROM %s ORDER BY partition";
    private static final String SILVER_SAMPLE_SQL = "SELECT * FROM %s ORDER BY event_time DESC LIMIT 5";
    private static final String GOLD_SAMPLE_SQL = "SELECT * FROM %s ORDER BY gmv DESC LIMIT 5";

    @Override
    public String name() {
        return LakeJobs.SHOW;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig platformConfig, Args args) {
        TableRef bronze = TableRef.of(platformConfig, LakeTables.BRONZE_ORDERS);
        TableRef silver = TableRef.of(platformConfig, LakeTables.SILVER_ORDERS);
        TableRef gold = TableRef.of(platformConfig, LakeTables.GOLD_DAILY_REVENUE);

        System.out.println("[show] ---- 表行数 ----");
        for (TableRef table : new TableRef[]{bronze, silver, gold}) {
            System.out.printf("  %-24s %,d 行%n", table.table(), spark.table(table.fqn()).count());
        }

        System.out.println("[show] ---- ops 元数据表 ----");
        opsCountLines(spark, platformConfig).forEach(System.out::println);

        System.out.println("[show] ---- gold 最近快照 ----");
        spark.sql(String.format(GOLD_SNAPSHOTS_SQL, gold.metadata("snapshots"))).show(false);

        System.out.println("[show] ---- bronze 提交历史 ----");
        spark.sql(String.format(BRONZE_HISTORY_SQL, bronze.metadata("history"))).show(false);

        System.out.println("[show] ---- bronze 分区布局 ----");
        spark.sql(String.format(BRONZE_PARTITIONS_SQL, bronze.metadata("partitions"))).show(false);

        System.out.println("[show] ---- silver 数据抽样 ----");
        spark.sql(String.format(SILVER_SAMPLE_SQL, silver.fqn())).show(false);

        System.out.println("[show] ---- gold 数据抽样 ----");
        spark.sql(String.format(GOLD_SAMPLE_SQL, gold.fqn())).show(false);
    }

    /** 包可见供巡检测试断言：四张 ops 表各一行「表名 + 行数」 */
    static List<String> opsCountLines(SparkSession spark, PlatformConfig platformConfig) {
        List<String> lines = new ArrayList<>();
        for (String table : OPS_TABLES) {
            TableRef ref = TableRef.of(platformConfig.catalogName(), LakeTables.OPS_SCHEMA, table);
            lines.add(String.format("  %-24s %,d 行", ref.table(), spark.table(ref.fqn()).count()));
        }
        return lines;
    }
}
