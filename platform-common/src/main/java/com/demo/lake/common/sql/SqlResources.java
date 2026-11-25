package com.demo.lake.common.sql;

import lombok.experimental.UtilityClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 外置 SQL 模板的注册表（单一事实源）。
 *
 * <p>资源放在 platform-common 的 classpath 下，Spark 作业、lake-adhoc 和单测拿到的是同一份。
 * 路径以 {@code _} 开头的文件是片段（如表属性、单个列定义），不能单独执行；
 * 其余文件每个都对应一条或多条可执行语句。</p>
 */
@UtilityClass
public final class SqlResources {

    public static final String DDL_DATABASES = "sql/ddl/00_databases.sql";
    public static final String DDL_BRONZE = "sql/ddl/10_bronze_orders.sql";
    public static final String DDL_SILVER = "sql/ddl/20_silver_orders.sql";
    public static final String DDL_GOLD = "sql/ddl/30_gold_daily_revenue.sql";
    public static final String DDL_BATCH_RUNS = "sql/ddl/40_ops_batch_runs.sql";
    public static final String DDL_DQ_RESULTS = "sql/ddl/50_ops_dq_results.sql";
    public static final String DDL_LINEAGE_RUNS = "sql/ddl/60_ops_lineage_runs.sql";
    public static final String DDL_RETENTION_AUDIT = "sql/ddl/70_ops_retention_audit.sql";

    public static final String VIEW_BRONZE_MASKED = "sql/view/10_bronze_orders_masked.sql";
    public static final String VIEW_BRONZE_RESTRICTED = "sql/view/11_bronze_orders_restricted.sql";
    public static final String VIEW_SILVER_MASKED = "sql/view/20_silver_orders_masked.sql";
    public static final String VIEW_SILVER_RESTRICTED = "sql/view/21_silver_orders_restricted.sql";

    public static final String PROPS_ROW_LEVEL = "sql/ddl/_properties_row_level.sql";
    public static final String PROPS_PLAIN = "sql/ddl/_properties_plain.sql";

    public static final String TRANSFORM_MERGE = "sql/transform/bronze_to_silver.sql";
    public static final String TRANSFORM_GOLD = "sql/transform/silver_to_gold.sql";

    public static final String MIGRATE_BATCH_RUNS_ADD = "sql/migrate/V1a__batch_runs_add_epoch.sql";
    public static final String MIGRATE_BATCH_RUNS_BACKFILL = "sql/migrate/V1b__batch_runs_backfill_epoch.sql";
    public static final String MIGRATE_COL_STARTED_MS = "sql/migrate/_col_started_ms.sql";
    public static final String MIGRATE_COL_FINISHED_MS = "sql/migrate/_col_finished_ms.sql";
    public static final String MIGRATE_SILVER_ADD_CHANNEL = "sql/migrate/V2a__silver_add_channel.sql";
    public static final String MIGRATE_SILVER_BACKFILL_CHANNEL = "sql/migrate/V2b__silver_backfill_channel.sql";
    public static final String MIGRATE_BRONZE_ADD_RUN_ID = "sql/migrate/V3__bronze_add_run_id.sql";

    public static final String QUALITY_SILVER_UNIQUE = "sql/quality/01_silver_unique.sql";
    public static final String QUALITY_SILVER_NOT_NULL = "sql/quality/02_silver_not_null.sql";
    public static final String QUALITY_SILVER_AMOUNT_RANGE = "sql/quality/03_silver_amount_range.sql";
    public static final String QUALITY_GOLD_REFUND_RATE = "sql/quality/04_gold_refund_rate.sql";
    public static final String QUALITY_GOLD_NET_REVENUE = "sql/quality/05_gold_net_revenue.sql";
    public static final String QUALITY_GOLD_ROW_COUNT = "sql/quality/06_gold_row_count.sql";

    private static final List<String> ALL = Collections.unmodifiableList(Arrays.asList(
            DDL_DATABASES, DDL_BRONZE, DDL_SILVER, DDL_GOLD, DDL_BATCH_RUNS, DDL_DQ_RESULTS,
            DDL_LINEAGE_RUNS, DDL_RETENTION_AUDIT,
            VIEW_BRONZE_MASKED, VIEW_BRONZE_RESTRICTED, VIEW_SILVER_MASKED, VIEW_SILVER_RESTRICTED,
            PROPS_ROW_LEVEL, PROPS_PLAIN,
            TRANSFORM_MERGE, TRANSFORM_GOLD,
            QUALITY_SILVER_UNIQUE, QUALITY_SILVER_NOT_NULL, QUALITY_SILVER_AMOUNT_RANGE,
            QUALITY_GOLD_REFUND_RATE, QUALITY_GOLD_NET_REVENUE, QUALITY_GOLD_ROW_COUNT,
            MIGRATE_BATCH_RUNS_ADD, MIGRATE_BATCH_RUNS_BACKFILL,
            MIGRATE_COL_STARTED_MS, MIGRATE_COL_FINISHED_MS,
            MIGRATE_SILVER_ADD_CHANNEL, MIGRATE_SILVER_BACKFILL_CHANNEL,
            MIGRATE_BRONZE_ADD_RUN_ID));

    /** 全部模板，含下划线开头的片段；单测遍历它保证没有漏登记、没有残留占位符。 */
    public static List<String> all() {
        return ALL;
    }

    /** 下划线开头的文件是供其他模板 include 的片段，不能单独执行。 */
    public static boolean isFragment(String resource) {
        int slash = resource.lastIndexOf('/');
        String name = slash < 0 ? resource : resource.substring(slash + 1);
        return name.startsWith("_");
    }
}
