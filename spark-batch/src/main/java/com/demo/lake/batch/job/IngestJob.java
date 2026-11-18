package com.demo.lake.batch.job;

import com.demo.lake.common.cli.Args;
import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;

/**
 * ODS 入湖：
 * 1) 先在 data/raw/orders/dt=YYYY-MM-DD/ 生成"上游落地文件"（Parquet），模拟数据到达；
 * 2) 读取落地目录，按 event_time 写入 Iceberg bronze 表 —— 分区由 Iceberg 自己路由，
 *    上游文件目录和 Iceberg 分区不一致也没关系（这正是表格式层带来的解耦）。
 *
 * 故意注入两类脏数据：
 * - 约 3% 订单有"重复记录"（先 PAID 后 REFUNDED），交给 DWD 的 MERGE 去重；
 * - 约 10% 记录 event_time 落在前一天（迟到数据），验证按事件时间分区而非按文件分区。
 */
public class IngestJob implements LakeJob {

    @Override
    public String name() {
        return "ingest";
    }

    @Override
    public void run(SparkSession spark, PlatformConfig cfg, Args args) throws Exception {
        int days = args.getInt("days", 7);
        int perDay = args.getInt("orders-per-day", 2000);
        Path rawRoot = Paths.get(cfg.get("platform.raw.dir", "data/raw"), "orders").toAbsolutePath();
        Files.createDirectories(rawRoot);

        System.out.println("[ingest] 落地目录: " + rawRoot);
        System.out.println("[ingest] 生成 " + days + " 天 x " + perDay + " 单的模拟落地文件");

        for (int offset = days - 1; offset >= 0; offset--) {
            LocalDate dt = LocalDate.now().minusDays(offset);
            Dataset<Row> df = buildDay(spark, offset, perDay);
            Path dir = rawRoot.resolve("dt=" + dt);
            df.write().mode("overwrite").option("compression", "snappy").parquet(dir.toString());
            System.out.printf("  dt=%s  行数=%d  -> %s%n", dt, df.count(), dir);
        }

        System.out.println("[ingest] 读取落地文件并写入 Iceberg（按 event_time 重新分区）");
        TableRef bronze = TableRef.of(cfg, LakeTables.BRONZE_ORDERS);
        Dataset<Row> raw = spark.read().option("mergeSchema", "true").parquet(rawRoot.toString());
        raw.createOrReplaceTempView("raw_orders");

        spark.sql("INSERT INTO " + bronze.fqn() + " "
                + "SELECT order_id, user_id, merchant_id, category, amount, currency, status, "
                + "       event_time, current_timestamp() AS ingest_time, input_file_name() AS source_file "
                + "FROM raw_orders");

        System.out.println("[ingest] bronze 总行数 = " + spark.table(bronze.fqn()).count());
        spark.sql("SELECT cast(event_time AS date) AS dt, count(*) AS cnt, count(DISTINCT order_id) AS distinct_orders "
                + "FROM " + bronze.fqn() + " GROUP BY 1 ORDER BY 1").show(false);
    }

    /** 生成单天落地数据（含重复与迟到记录） */
    private Dataset<Row> buildDay(SparkSession spark, int dayOffset, int rows) {
        // 种子按天偏移，保证不同天金额分布不同、同一天可复现
        int seed = 100 + dayOffset * 10;
        Dataset<Row> base = spark.range(rows).selectExpr(
                "id",
                "rand(" + seed + ") AS r_user",
                "rand(" + (seed + 1) + ") AS r_merchant",
                "rand(" + (seed + 2) + ") AS r_category",
                "rand(" + (seed + 3) + ") AS r_amount",
                "rand(" + (seed + 4) + ") AS r_status",
                "rand(" + (seed + 5) + ") AS r_late",
                "rand(" + (seed + 6) + ") AS r_hour",
                "rand(" + (seed + 7) + ") AS r_dup");

        // 10% 的记录 event_time 落在前一天（迟到数据）
        String dayExpr = "date_sub(current_date(), " + dayOffset
                + " + CASE WHEN r_late < 0.10 THEN 1 ELSE 0 END)";

        Dataset<Row> clean = base.selectExpr(
                "concat('ORD-', '" + dayOffset + "-', lpad(cast(id AS string), 7, '0')) AS order_id",
                "concat('U', lpad(cast(cast(r_user * 5000 AS int) AS string), 5, '0')) AS user_id",
                "concat('M', lpad(cast(cast(r_merchant * 20 AS int) AS string), 3, '0')) AS merchant_id",
                "element_at(array('3C','FASHION','GROCERY','HOME','BEAUTY'), "
                        + "cast(r_category * 5 AS int) + 1) AS category",
                "cast(round(10 + r_amount * 990, 2) AS decimal(12,2)) AS amount",
                "'CNY' AS currency",
                "CASE WHEN r_status < 0.06 THEN 'REFUNDED' "
                        + "WHEN r_status < 0.10 THEN 'CANCELLED' ELSE 'PAID' END AS status",
                "to_timestamp(concat(cast(" + dayExpr + " AS string), ' ', "
                        + "lpad(cast(cast(r_hour * 24 AS int) AS string), 2, '0'), ':00:00')) AS event_time",
                "r_dup");

        // 约 3% 的 PAID 单再造一条 2 小时后的 REFUNDED 记录，制造重复
        Dataset<Row> refunded = clean
                .where("r_dup < 0.03 AND status = 'PAID'")
                .drop("r_dup")
                .withColumn("status", functions.lit("REFUNDED"))
                .withColumn("event_time", functions.expr("event_time + INTERVAL 2 HOURS"));

        return clean.drop("r_dup").unionByName(refunded);
    }
}
