package com.demo.lake.common.catalog;

import com.demo.lake.common.config.PlatformConfig;

/**
 * 分层转换 SQL（Spark SQL / Iceberg 语法）。
 * 抽到 common 里，Spark 作业和单测共用同一份 SQL，避免"测试用的 SQL 和线上跑的不一样"。
 */
public final class TransformSql {

    private TransformSql() {
    }

    /**
     * ODS -&gt; DWD：按 order_id 取最新事件做 upsert。
     * 只扫最近 days 天的分区，靠 Iceberg 的分区裁剪控制扫描量。
     */
    public static String mergeBronzeIntoSilver(PlatformConfig cfg, int days) {
        TableRef bronze = TableRef.of(cfg, LakeTables.BRONZE_ORDERS);
        TableRef silver = TableRef.of(cfg, LakeTables.SILVER_ORDERS);
        return "MERGE INTO " + silver.fqn() + " AS t\n"
                + "USING (\n"
                + "  SELECT order_id, user_id, merchant_id, category, amount, currency, status,\n"
                + "         status = 'REFUNDED' AS is_refund,\n"
                + "         CAST(event_time AS DATE) AS dt,\n"
                + "         event_time, CURRENT_TIMESTAMP() AS updated_at\n"
                + "  FROM (\n"
                + "    SELECT b.*, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY event_time DESC, ingest_time DESC) AS rn\n"
                + "    FROM " + bronze.fqn() + " b\n"
                + "    WHERE event_time >= CURRENT_DATE() - INTERVAL " + days + " DAYS\n"
                + "  ) WHERE rn = 1\n"
                + ") AS s\n"
                + "ON t.order_id = s.order_id\n"
                + "WHEN MATCHED AND s.event_time > t.event_time THEN UPDATE SET *\n"
                + "WHEN NOT MATCHED THEN INSERT *";
    }

    /**
     * DWD -&gt; ADS：按天/商户/品类重算汇总。
     * INSERT OVERWRITE 在 Iceberg 里是动态分区覆盖，只替换出现的分区；
     * 若要按天增量，在 WHERE 里加 dt 条件即可，例如 dt = current_date - 1。
     *
     * <p>注意：buyer_cnt 是 (dt, 商户, 品类) 粒度下的去重用户数，跨行相加得到的是“人次”，
     * 不是全站去重买家数。</p>
     */
    public static String rebuildGold(PlatformConfig cfg) {
        TableRef silver = TableRef.of(cfg, LakeTables.SILVER_ORDERS);
        TableRef gold = TableRef.of(cfg, LakeTables.GOLD_DAILY_REVENUE);
        return "INSERT OVERWRITE " + gold.fqn() + "\n"
                + "SELECT dt, merchant_id, category,\n"
                + "       COUNT(*) AS order_cnt,\n"
                + "       COUNT(DISTINCT user_id) AS buyer_cnt,\n"
                + "       CAST(SUM(amount) AS DECIMAL(18,2)) AS gmv,\n"
                + "       CAST(SUM(CAST(CASE WHEN is_refund THEN amount ELSE CAST(0 AS DECIMAL(12,2)) END"
                + " AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS refund_amount,\n"
                + "       CAST(SUM(CASE WHEN is_refund THEN CAST(0 AS DECIMAL(12,2)) ELSE amount END)"
                + " AS DECIMAL(18,2)) AS net_revenue,\n"
                + "       CURRENT_TIMESTAMP() AS updated_at\n"
                + "FROM " + silver.fqn() + "\n"
                + "WHERE status <> 'CANCELLED'\n"
                + "GROUP BY dt, merchant_id, category";
    }
}
