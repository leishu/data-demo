package com.demo.lake.batch.job;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.cli.Args;
import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.SparkSession;

/** 只读巡检：快照、历史、分区、文件布局。Presto 侧也能查同样的元数据表 */
public class ShowJob implements LakeJob {

    @Override
    public String name() {
        return LakeJobs.SHOW;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig cfg, Args args) {
        TableRef bronze = TableRef.of(cfg, LakeTables.BRONZE_ORDERS);
        TableRef silver = TableRef.of(cfg, LakeTables.SILVER_ORDERS);
        TableRef gold = TableRef.of(cfg, LakeTables.GOLD_DAILY_REVENUE);

        System.out.println("[show] ---- 表行数 ----");
        for (TableRef t : new TableRef[]{bronze, silver, gold}) {
            System.out.printf("  %-24s %,d 行%n", t.table(), spark.table(t.fqn()).count());
        }

        System.out.println("[show] ---- gold 最近快照 ----");
        spark.sql("SELECT snapshot_id, parent_id, operation, "
                + "summary['added-records'] AS added, summary['deleted-records'] AS deleted, "
                + "summary['total-records'] AS total, committed_at "
                + "FROM " + gold.metadata("snapshots") + " ORDER BY committed_at DESC LIMIT 8").show(false);

        System.out.println("[show] ---- bronze 提交历史 ----");
        spark.sql("SELECT made_current_at, snapshot_id, is_current_ancestor "
                + "FROM " + bronze.metadata("history") + " ORDER BY made_current_at DESC LIMIT 8").show(false);

        System.out.println("[show] ---- bronze 分区布局 ----");
        spark.sql("SELECT partition, record_count, file_count, total_data_file_size_in_bytes "
                + "FROM " + bronze.metadata("partitions") + " ORDER BY partition").show(false);

        System.out.println("[show] ---- silver 数据抽样 ----");
        spark.sql("SELECT * FROM " + silver.fqn() + " ORDER BY event_time DESC LIMIT 5").show(false);

        System.out.println("[show] ---- gold 数据抽样 ----");
        spark.sql("SELECT * FROM " + gold.fqn() + " ORDER BY gmv DESC LIMIT 5").show(false);
    }
}
