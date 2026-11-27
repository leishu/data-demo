package com.demo.lake.common.catalog;

import com.demo.lake.common.config.PlatformConfig;
import lombok.experimental.UtilityClass;
import com.demo.lake.common.sql.SqlResources;
import com.demo.lake.common.sql.SqlScript;
import com.demo.lake.common.sql.SqlTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 存量表结构迁移 SQL（Spark SQL / Iceberg 语法）。
 *
 * <p>语句文本放在 classpath 的 {@code sql/migrate/} 下，按 V1/V2 编号。
 * 本类只负责绑定表名、组装缺失列，以及把「列是否已存在 / 是否有旧行要回填」的判断
 * 留给调用方（InitJob / EvolveJob）——SQL 文件里不放控制流。</p>
 */
@UtilityClass
public final class Migrations {

    /** 支持按列名取到对应的列定义片段，避免把 DDL 片段重新写回 Java。 */
    private static final Map<String, String> COLUMN_FRAGMENTS = new LinkedHashMap<>();

    static {
        COLUMN_FRAGMENTS.put("started_ms", SqlResources.MIGRATE_COL_STARTED_MS);
        COLUMN_FRAGMENTS.put("finished_ms", SqlResources.MIGRATE_COL_FINISHED_MS);
    }

    /** 取迁移用的列定义（不含前后逗号，由调用方 join）。列名未在注册表中时直接报错。 */
    public static String columnDefinition(String column) {
        String resource = COLUMN_FRAGMENTS.get(column);
        if (resource == null) {
            throw new IllegalArgumentException("未知的迁移列定义: " + column);
        }
        return SqlScript.readResource(resource).trim();
    }

    /** V1a：只补实际缺失的列；missingColumns 为空时调用方不应执行本语句。 */
    public static String addBatchRunsEpochColumns(PlatformConfig platformConfig, List<String> missingColumns) {
        Map<String, String> vars = vars(platformConfig);
        vars.put("columns", String.join(", ", missingColumns));
        return one(SqlResources.MIGRATE_BATCH_RUNS_ADD, vars);
    }

    /** V1b：按 UTC 墙钟回填旧行的 epoch 列，只更新仍为 NULL 的行，可重复执行。 */
    public static String backfillBatchRunsEpoch(PlatformConfig platformConfig) {
        return one(SqlResources.MIGRATE_BATCH_RUNS_BACKFILL, vars(platformConfig));
    }

    /** V2a：silver 加 channel 列（纯元数据变更）。 */
    public static String addSilverChannel(PlatformConfig platformConfig) {
        return one(SqlResources.MIGRATE_SILVER_ADD_CHANNEL, vars(platformConfig));
    }

    /** V2b：回填 channel，CoW 会重写受影响的数据文件。 */
    public static String backfillSilverChannel(PlatformConfig platformConfig) {
        return one(SqlResources.MIGRATE_SILVER_BACKFILL_CHANNEL, vars(platformConfig));
    }

    /** V3：bronze 补 run_id 列（加列前的历史行由 IngestJob 删除）。 */
    public static String addBronzeRunId(PlatformConfig platformConfig) {
        return one(SqlResources.MIGRATE_BRONZE_ADD_RUN_ID, vars(platformConfig));
    }

    private static Map<String, String> vars(PlatformConfig platformConfig) {
        Map<String, String> vars = new LinkedHashMap<>(LakeTables.baseVars(platformConfig));
        vars.put("batch_runs_table",
                TableRef.of(platformConfig.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.BATCH_RUNS).fqn());
        vars.put("bronze_table", TableRef.of(platformConfig, LakeTables.BRONZE_ORDERS).fqn());
        vars.put("silver_table", TableRef.of(platformConfig, LakeTables.SILVER_ORDERS).fqn());
        return vars;
    }

    private static String one(String resource, Map<String, String> vars) {
        List<String> statements = SqlScript.split(SqlTemplate.renderResource(resource, vars));
        if (statements.size() != 1) {
            throw new IllegalStateException("迁移 SQL 模板应只有一条语句: " + resource);
        }
        // 同上：模板注释不进 spark.sql()
        return SqlScript.stripComments(statements.get(0)).trim();
    }
}
