package com.demo.lake.batch.job;

import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.catalog.TransformSql;
import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.SparkSession;

/**
 * 批处理主链路：
 * bronze(ODS) --MERGE--> silver(DWD) --INSERT OVERWRITE--> gold(ADS)
 *
 * 用 MERGE 而不是全量覆盖，是为了把"迟到数据 / 重复数据"的修正代价限制在变化的行上。
 */
public class TransformJob implements LakeJob {

    private static final String GOLD_TOP_SQL =
            "SELECT dt, merchant_id, category, order_cnt, buyer_cnt, gmv, refund_amount, net_revenue "
            + "FROM %s ORDER BY gmv DESC LIMIT 10";
    private static final String GOLD_DAILY_SQL =
            "SELECT dt, SUM(order_cnt) AS orders, SUM(buyer_cnt) AS buyer_cnt_sum, "
            + "CAST(SUM(gmv) AS DECIMAL(18,2)) AS gmv, CAST(SUM(net_revenue) AS DECIMAL(18,2)) AS net_revenue "
            + "FROM %s GROUP BY dt ORDER BY dt";

    @Override
    public String name() {
        return LakeJobs.TRANSFORM;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig platformConfig, Args args) {
        int days = args.getInt("days", 7);

        TableRef silver = TableRef.of(platformConfig, LakeTables.SILVER_ORDERS);
        TableRef gold = TableRef.of(platformConfig, LakeTables.GOLD_DAILY_REVENUE);

        long silverBefore = spark.table(silver.fqn()).count();
        System.out.println("[transform] MERGE bronze -> silver（去重 + upsert）");
        spark.sql(TransformSql.mergeBronzeIntoSilver(platformConfig, days));

        long silverAfter = spark.table(silver.fqn()).count();
        System.out.printf("[transform] silver: %d -> %d 行（新增 %d）%n",
                silverBefore, silverAfter, silverAfter - silverBefore);

        System.out.println("[transform] INSERT OVERWRITE silver -> gold（按天/商户/品类聚合）");
        spark.sql(TransformSql.rebuildGold(platformConfig));

        System.out.println("[transform] gold 明细 Top 10（按 GMV）：");
        spark.sql(String.format(GOLD_TOP_SQL, gold.fqn())).show(false);

        System.out.println("[transform] gold 每日大盘：");
        spark.sql(String.format(GOLD_DAILY_SQL, gold.fqn())).show(false);
    }
}
