package com.demo.lake.batch.job;

import com.demo.lake.batch.LakeJob;
import com.demo.lake.batch.SparkSnapshots;
import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.batch.RunIds;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.quality.QualityCheck;
import com.demo.lake.common.quality.QualityChecks;
import com.demo.lake.common.quality.QualitySql;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 数据质量检查：跑 common 登记的六个检查项，结果写 {@code ops.dq_results}。
 *
 * <p>每个检查 SQL 都返回 {@code observed / threshold / detail}，observed 越大越差，
 * {@code observed > threshold} 即失败。ERROR 项失败会让作业抛异常（BatchApp 落 FAILED 终态），
 * WARN 项只记录不阻断。</p>
 *
 * <p>{@code --inject-faults} 是演示开关：往 silver 注入重复订单号与负金额，
 * 让检查能稳定产出失败证据（治理页按 run_id 就能查到）。</p>
 */
public class QualityJob implements LakeJob {

    @Override
    public String name() {
        return LakeJobs.QUALITY;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig platformConfig, Args args) {
        TableRef results = TableRef.of(platformConfig.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.DQ_RESULTS);
        if (!spark.catalog().tableExists(results.fqn())) {
            throw new IllegalStateException("质量结果表不存在，先跑一次 init: " + results.fqn());
        }
        String runId = RunIds.resolve(args);
        if (args.flag("inject-faults")) {
            injectFaults(spark, platformConfig);
        }

        List<Row> rows = new ArrayList<>();
        int failedErrors = 0;
        int failedWarns = 0;
        for (QualityCheck check : QualityChecks.ALL) {
            Row observed = spark.sql(QualitySql.render(check, platformConfig)).head();
            // Spark 把 0.0 / 1.0 这类字面量解析成 DECIMAL，按 Number 读兼容 DOUBLE / DECIMAL
            double value = ((Number) observed.get(0)).doubleValue();
            double threshold = ((Number) observed.get(1)).doubleValue();
            String detail = observed.get(2) == null ? "" : String.valueOf(observed.get(2));
            boolean failed = value > threshold;
            rows.add(RowFactory.create(runId, snapshotId(spark, platformConfig, check), check.name(),
                    check.severity().name(), value, threshold, detail, System.currentTimeMillis()));
            System.out.printf("[quality] %-20s %-6s observed=%.4f threshold=%.4f -> %s (%s)%n",
                    check.name(), check.severity(), value, threshold,
                    failed ? "FAIL" : "PASS", detail.isEmpty() ? "-" : detail);
            if (failed && check.severity().failsJob()) {
                failedErrors++;
            } else if (failed) {
                failedWarns++;
            }
        }

        write(spark, results, rows);
        System.out.printf("[quality] 结果已写入 %s（run_id=%s，ERROR 失败 %d 项，WARN 失败 %d 项）%n",
                results.fqn(), runId, failedErrors, failedWarns);
        if (failedErrors > 0) {
            throw new IllegalStateException("数据质量检查失败: " + failedErrors + " 项 ERROR，详见 "
                    + results.fqn() + " run_id=" + runId);
        }
    }

    /** 快照 ID 取被检查来源表的最近一次提交，让结果能定位到具体数据版本 */
    private static String snapshotId(SparkSession spark, PlatformConfig platformConfig, QualityCheck check) {
        String table = "silver".equals(check.source())
                ? TableRef.of(platformConfig, LakeTables.SILVER_ORDERS).fqn()
                : TableRef.of(platformConfig, LakeTables.GOLD_DAILY_REVENUE).fqn();
        try {
            return SparkSnapshots.latestId(spark, table);
        } catch (RuntimeException e) {
            // 证据 informational：快照读不到不该让质量检查本身失败
            return null;
        }
    }

    private void write(SparkSession spark, TableRef results, List<Row> rows) {
        StructType schema = new StructType()
                .add("run_id", DataTypes.StringType, false)
                .add("snapshot_id", DataTypes.StringType, true)
                .add("check", DataTypes.StringType, false)
                .add("severity", DataTypes.StringType, false)
                .add("observed", DataTypes.DoubleType, false)
                .add("threshold", DataTypes.DoubleType, false)
                .add("detail", DataTypes.StringType, true)
                .add("checked_ms", DataTypes.LongType, false);
        try {
            spark.createDataFrame(rows, schema).writeTo(results.fqn()).append();
        } catch (Exception e) {
            // writeTo 声明了 checked 的 NoSuchTableException：结果写不进去就必须让作业失败，
            // 否则页面会拿不到任何证据，还以为检查全过了。
            throw new IllegalStateException("写入质量检查结果失败: " + results.fqn(), e);
        }
    }

    /**
     * 演示用的脏数据：两条同 order_id 的重复单 + 一条负金额单。
     * 显式列列表，gold/silver 之后加列也不会因为列数不匹配而挂。
     */
    private void injectFaults(SparkSession spark, PlatformConfig platformConfig) {
        String silver = TableRef.of(platformConfig, LakeTables.SILVER_ORDERS).fqn();
        String columns = "(order_id, user_id, merchant_id, category, amount, currency, status, "
                + "is_refund, dt, event_time, updated_at)";
        String badRows = "('DQ-DUP','U1','M1','3C', CAST(10.00 AS DECIMAL(12,2)), 'CNY','PAID',false, "
                + "date_sub(current_date(),1), current_timestamp(), current_timestamp()), "
                + "('DQ-DUP','U1','M1','3C', CAST(10.00 AS DECIMAL(12,2)), 'CNY','PAID',false, "
                + "date_sub(current_date(),1), current_timestamp(), current_timestamp()), "
                + "('DQ-NEG','U2','M1','3C', CAST(-50.00 AS DECIMAL(12,2)), 'CNY','PAID',false, "
                + "date_sub(current_date(),1), current_timestamp(), current_timestamp())";
        spark.sql("INSERT INTO " + silver + " " + columns + " VALUES " + badRows);
        System.out.println("[quality] 已注入演示脏数据：重复单 DQ-DUP ×2、负金额单 DQ-NEG");
    }
}
