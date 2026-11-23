package com.demo.lake.batch.job;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.batch.RunIds;
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
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ODS 入湖：
 * 1) 先在 platform.raw.dir（默认 data/raw）/orders/dt=YYYY-MM-DD/ 生成"上游落地文件"（Parquet），
 *    模拟数据到达；每个 dt 目录是整目录覆盖写，窗口以运行当天为终点往前推 --days 天（含今天），
 *    所以隔天再跑会与上一次重叠 days-1 天；
 * 2) 只读取本次生成的那些 dt 目录，按 event_time 写入 Iceberg bronze 表 —— 分区由 Iceberg
 *    自己路由，上游文件目录和 Iceberg 分区不一致也没关系（这正是表格式层带来的解耦）。
 *
 * <p>注意两层语义不同：raw 目录是覆盖，bronze 是 INSERT INTO 纯追加。同一批日期重跑（或每天调度
 * 时窗口重叠）会在 bronze 里叠出重复行，去重靠 DWD 的 MERGE 按 order_id 收敛；ODS 自身只追加、
 * 不保证幂等。</p>
 *
 * <p>order_id 里带业务日期、随机种子也按日期取，保证同一批落地数据在 --orders-per-day 不变时重跑
 * 得到相同的 ID 和数据。</p>
 *
 * <p>每行带 run_id（与 {@code ops.batch_runs.run_id} 同一个值），可据此判断数据是哪次任务导入的。
 * 老仓库首次跑本作业时会先 ADD COLUMNS 补列，并删掉无法归属批次的旧行（run_id 为空）。</p>
 *
 * 故意注入两类脏数据：
 * - 约 3% 的 PAID 单（约占总行数 2.7%）有"重复记录"（先 PAID 后 REFUNDED），
 *   交给 DWD 的 MERGE 收敛；
 * - 约 10% 记录 event_time 落在前一天（迟到数据），验证按事件时间分区而非按文件分区。
 */
public class IngestJob implements LakeJob {

    @Override
    public String name() {
        return LakeJobs.INGEST;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig cfg, Args args) throws Exception {
        int days = args.getInt("days", 7);
        int perDay = args.getInt("orders-per-day", 2000);
        String runId = RunIds.resolve(args);
        Path rawRoot = Paths.get(cfg.get("platform.raw.dir", "data/raw"), "orders").toAbsolutePath();
        Files.createDirectories(rawRoot);

        System.out.println("[ingest] 落地目录: " + rawRoot);
        System.out.println("[ingest] run_id=" + runId);
        System.out.println("[ingest] 生成 " + days + " 天 x " + perDay + " 单的模拟落地文件");

        List<String> generated = new ArrayList<>();
        for (int offset = days - 1; offset >= 0; offset--) {
            // Spark 会话固定 UTC（PlatformConfig 的 spark.sql.session.timeZone），业务日期也必须用 UTC，
            // 否则非 UTC 机器在跨零点时会给 dt 打上本地日期，和 event_time / CURRENT_DATE 错位
            LocalDate dt = LocalDate.now(ZoneOffset.UTC).minusDays(offset);
            Dataset<Row> df = buildDay(spark, dt, perDay);
            Path dir = rawRoot.resolve("dt=" + dt);
            df.write().mode("overwrite").option("compression", "snappy").parquet(dir.toString());
            generated.add(dir.toString());
            System.out.printf("  dt=%s  行数=%d  -> %s%n", dt, df.count(), dir);
        }

        System.out.println("[ingest] 读取本次落地文件并写入 Iceberg（按 event_time 重新分区）");
        TableRef bronze = TableRef.of(cfg, LakeTables.BRONZE_ORDERS);
        prepareBronze(spark, bronze);
        Dataset<Row> raw = spark.read().option("mergeSchema", "true")
                .parquet(generated.toArray(new String[0]));
        raw.withColumn("run_id", functions.lit(runId)).createOrReplaceTempView("raw_orders");

        // 纯追加：重跑会把同一批日期再叠一份，幂等去重交给 DWD 的 MERGE
        spark.sql("INSERT INTO " + bronze.fqn() + " "
                + "(order_id, user_id, merchant_id, category, amount, currency, status, "
                + " event_time, ingest_time, source_file, run_id) "
                + "SELECT order_id, user_id, merchant_id, category, amount, currency, status, "
                + "       event_time, CURRENT_TIMESTAMP() AS ingest_time, INPUT_FILE_NAME() AS source_file, run_id "
                + "FROM raw_orders");

        System.out.println("[ingest] bronze 总行数 = " + spark.table(bronze.fqn()).count());
        spark.sql("SELECT CAST(event_time AS DATE) AS dt, COUNT(*) AS cnt, COUNT(DISTINCT order_id) AS distinct_orders "
                + "FROM " + bronze.fqn() + " GROUP BY 1 ORDER BY 1").show(false);
    }

    /**
     * 老仓库升级：run_id 是后加的列，旧表先 ADD COLUMNS 补上（纯元数据变更），
     * 再加列前写入的历史行没有 run_id，按「旧数据删除」清掉，保证表里每行都能追到批次。
     */
    private void prepareBronze(SparkSession spark, TableRef bronze) {
        if (!spark.catalog().tableExists(bronze.fqn())) {
            return; // 表还没建：init 会按最新 DDL 建好，这里不越权
        }
        if (!Arrays.asList(spark.table(bronze.fqn()).columns()).contains("run_id")) {
            System.out.println("[ingest] 旧表结构缺少 run_id，ADD COLUMNS 补列（仅元数据变更）");
            spark.sql("ALTER TABLE " + bronze.fqn()
                    + " ADD COLUMNS (run_id STRING COMMENT '入湖批次运行ID（对应 ops.batch_runs.run_id）')");
        }
        long legacy = spark.table(bronze.fqn()).filter("run_id IS NULL").count();
        if (legacy > 0) {
            System.out.println("[ingest] 删除无法归属批次的旧数据 " + legacy + " 行（run_id IS NULL）");
            spark.sql("DELETE FROM " + bronze.fqn() + " WHERE run_id IS NULL");
        }
    }

    /** 生成单天落地数据（含重复与迟到记录）；同一天重跑必须得到同一份数据 */
    private Dataset<Row> buildDay(SparkSession spark, LocalDate dt, int rows) {
        // 种子按业务日期取，保证不同天分布不同、同一天（哪怕隔天重跑）可复现
        int seed = 100 + (int) (Math.floorMod(dt.toEpochDay(), 100_000L) * 10);
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

        // 值走 ? 绑定：selectExpr 没有带参重载，先把 base 挂成按日期命名的临时视图，
        // 再用 spark.sql(sql, args) 绑 dt；每天一个视图互不覆盖（下游 Dataset 是懒执行的）。
        String view = "ingest_base_" + dt.toString().replace("-", "");
        base.createOrReplaceTempView(view);

        // 10% 的记录 event_time 落在前一天（迟到数据）
        Dataset<Row> clean = spark.sql(
                "SELECT concat('ORD-', ?, '-', lpad(cast(id AS string), 7, '0')) AS order_id,"
                        + " concat('U', lpad(cast(cast(r_user * 5000 AS int) AS string), 5, '0')) AS user_id,"
                        + " concat('M', lpad(cast(cast(r_merchant * 20 AS int) AS string), 3, '0')) AS merchant_id,"
                        + " element_at(array('3C','FASHION','GROCERY','HOME','BEAUTY'), "
                        + "cast(r_category * 5 AS int) + 1) AS category,"
                        + " cast(round(10 + r_amount * 990, 2) AS decimal(12,2)) AS amount,"
                        + " 'CNY' AS currency,"
                        + " CASE WHEN r_status < 0.06 THEN 'REFUNDED' "
                        + "WHEN r_status < 0.10 THEN 'CANCELLED' ELSE 'PAID' END AS status,"
                        + " to_timestamp(concat(cast(date_sub(CAST(? AS DATE), "
                        + "CASE WHEN r_late < 0.10 THEN 1 ELSE 0 END) AS string), ' ', "
                        + "lpad(cast(cast(r_hour * 24 AS int) AS string), 2, '0'), ':00:00')) AS event_time,"
                        + " r_dup"
                        + " FROM " + view,
                new Object[]{dt.toString(), dt.toString()});

        // 约 3% 的 PAID 单再造一条 2 小时后的 REFUNDED 记录，制造重复
        Dataset<Row> refunded = clean
                .where("r_dup < 0.03 AND status = 'PAID'")
                .drop("r_dup")
                .withColumn("status", functions.lit("REFUNDED"))
                .withColumn("event_time", functions.expr("event_time + INTERVAL 2 HOURS"));

        return clean.drop("r_dup").unionByName(refunded);
    }
}
