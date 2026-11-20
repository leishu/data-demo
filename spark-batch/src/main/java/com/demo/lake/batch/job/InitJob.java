package com.demo.lake.batch.job;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.cli.Args;
import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlScript;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** 建库建表：ODS / DWD / ADS 三层。幂等，可重复执行 */
public class InitJob implements LakeJob {

    @Override
    public String name() {
        return LakeJobs.INIT;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig cfg, Args args) {
        System.out.println("[init] catalog=" + cfg.catalogName() + " database=" + cfg.database()
                + " warehouse=" + cfg.warehouse());

        for (String ddl : LakeTables.all(cfg)) {
            System.out.println("  DDL> " + SqlScript.oneLine(ddl, 110));
            spark.sql(ddl);
        }
        // CREATE TABLE IF NOT EXISTS 不会改老表结构：epoch 时间列单独补
        prepareBatchRuns(spark, cfg);

        System.out.println("[init] 当前库表：");
        spark.sql("SHOW TABLES IN " + cfg.catalogName() + "." + cfg.database()).show(false);
    }

    /**
     * 老表升级：时间的权威表示从「UTC 墙钟 timestamp」改成「epoch 毫秒 bigint」。
     * 先 ADD COLUMNS（纯元数据），再把旧行的 epoch 按 UTC 墙钟回填——
     * 会话时区在 PlatformConfig 里钉死为 UTC，所以 unix_millis 的解释是无歧义的。
     */
    private void prepareBatchRuns(SparkSession spark, PlatformConfig cfg) {
        TableRef table = TableRef.of(cfg.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.BATCH_RUNS);
        if (!spark.catalog().tableExists(table.fqn())) {
            return; // 表还没建：上面的 CREATE TABLE 已经是最新结构
        }
        List<String> columns = Arrays.asList(spark.table(table.fqn()).columns());
        List<String> missing = new ArrayList<>();
        if (!columns.contains("started_ms")) {
            missing.add("started_ms BIGINT COMMENT '权威开始时间：epoch 毫秒（UTC 绝对时刻）'");
        }
        if (!columns.contains("finished_ms")) {
            missing.add("finished_ms BIGINT COMMENT '权威结束时间：epoch 毫秒（UTC 绝对时刻）'");
        }
        if (!missing.isEmpty()) {
            System.out.println("[init] ops.batch_runs 缺少 epoch 时间列，ADD COLUMNS: " + missing);
            spark.sql("ALTER TABLE " + table.fqn() + " ADD COLUMNS (" + String.join(", ", missing) + ")");
        }
        long legacy = spark.table(table.fqn()).filter("started_ms IS NULL OR finished_ms IS NULL").count();
        if (legacy > 0) {
            System.out.println("[init] 回填旧运行记录的时间列 " + legacy + " 行（按 UTC 墙钟解释旧 timestamp）");
            spark.sql("UPDATE " + table.fqn()
                    + " SET started_ms = unix_millis(started_at), finished_ms = unix_millis(finished_at)"
                    + " WHERE started_ms IS NULL OR finished_ms IS NULL");
        }
    }
}
