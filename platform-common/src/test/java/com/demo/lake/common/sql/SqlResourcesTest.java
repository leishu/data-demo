package com.demo.lake.common.sql;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.Migrations;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.catalog.TransformSql;
import com.demo.lake.common.config.PlatformConfig;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 外置 SQL 模板的守卫：模板拼错、占位符写错、表名与 Java 常量漂移都会在这里挂掉。 */
class SqlResourcesTest {

    private static PlatformConfig cfg() {
        return PlatformConfig.load();
    }

    /** 覆盖所有模板可能用到的占位符；值本身不重要，这里只校验结构。 */
    private static Map<String, String> allVars(PlatformConfig cfg) {
        Map<String, String> vars = new LinkedHashMap<>(LakeTables.baseVars(cfg));
        vars.put("table_properties", "'format-version' = '2'");
        vars.put("days", "7");
        vars.put("columns", "started_ms BIGINT, finished_ms BIGINT");
        vars.put("bronze_table", TableRef.of(cfg, LakeTables.BRONZE_ORDERS).fqn());
        vars.put("silver_table", TableRef.of(cfg, LakeTables.SILVER_ORDERS).fqn());
        vars.put("gold_table", TableRef.of(cfg, LakeTables.GOLD_DAILY_REVENUE).fqn());
        vars.put("batch_runs_table",
                TableRef.of(cfg.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.BATCH_RUNS).fqn());
        return vars;
    }

    @Test
    void everyRegisteredTemplateLoadsWithoutUnresolvedPlaceholders() {
        Map<String, String> vars = allVars(cfg());
        for (String resource : SqlResources.all()) {
            String rendered = SqlTemplate.renderResource(resource, vars);
            assertFalse(rendered.trim().isEmpty(), resource + " 渲染结果为空");
            assertFalse(SqlTemplate.hasUnresolved(rendered),
                    resource + " 残留未解析占位符: " + SqlScript.oneLine(rendered, 120));
            if (!SqlResources.isFragment(resource)) {
                assertFalse(SqlScript.split(rendered).isEmpty(), resource + " 没有可执行语句");
            }
        }
    }

    @Test
    void ddlRenderingKeepsTableNamesAndPropertyDifferences() {
        PlatformConfig cfg = cfg();
        List<String> ddl = LakeTables.all(cfg);
        assertEquals(9, ddl.size()); // 2 建库 + 7 建表（业务 3 + ops 4）
        for (String stmt : ddl) {
            assertFalse(SqlTemplate.hasUnresolved(stmt), "DDL 残留占位符: " + stmt);
        }
        // 00_databases.sql 里有两条建库语句，表从索引 2 开始
        assertTrue(ddl.get(0).contains("CREATE DATABASE"));
        assertTrue(ddl.get(1).contains("CREATE DATABASE"));
        assertTrue(ddl.get(2).contains(LakeTables.BRONZE_ORDERS));
        assertTrue(ddl.get(3).contains(LakeTables.SILVER_ORDERS));
        assertTrue(ddl.get(4).contains(LakeTables.GOLD_DAILY_REVENUE));
        assertTrue(ddl.get(5).contains(LakeTables.OPS_SCHEMA + "." + LakeTables.BATCH_RUNS));
        assertTrue(ddl.get(6).contains(LakeTables.OPS_SCHEMA + "." + LakeTables.DQ_RESULTS));
        assertTrue(ddl.get(7).contains(LakeTables.OPS_SCHEMA + "." + LakeTables.LINEAGE_RUNS));
        assertTrue(ddl.get(8).contains(LakeTables.OPS_SCHEMA + "." + LakeTables.RETENTION_AUDIT));

        // bronze/silver 有行级写入属性，gold/ops 没有
        assertTrue(ddl.get(2).contains("'write.merge.mode'"));
        assertTrue(ddl.get(3).contains("'write.delete.mode'"));
        assertFalse(ddl.get(4).contains("'write.merge.mode'"));
        assertFalse(ddl.get(5).contains("'write.delete.mode'"));
    }

    @Test
    void transformRenderingSubstitutesDaysAndTableNames() {
        PlatformConfig cfg = cfg();
        String merge = TransformSql.mergeBronzeIntoSilver(cfg, 7);
        assertTrue(merge.contains("INTERVAL 7 DAYS"));
        assertTrue(merge.contains(TableRef.of(cfg, LakeTables.SILVER_ORDERS).fqn()));
        assertFalse(SqlTemplate.hasUnresolved(merge));
        // 回归：target 会被加列，MERGE 必须显式列，不能用依赖完整 schema 的 *
        assertFalse(merge.contains("UPDATE SET *"), "MERGE 不能用 UPDATE SET *");
        assertFalse(merge.contains("INSERT *"), "MERGE 不能用 INSERT *");
        assertTrue(merge.contains("WHEN NOT MATCHED THEN INSERT ("));

        String gold = TransformSql.rebuildGold(cfg);
        assertTrue(gold.contains(TableRef.of(cfg, LakeTables.GOLD_DAILY_REVENUE).fqn()));
        assertFalse(SqlTemplate.hasUnresolved(gold));
        // 回归：INSERT OVERWRITE 必须带列列表，不能靠位置匹配（gold 加列后会挂）
        assertTrue(gold.contains("INSERT OVERWRITE "
                        + TableRef.of(cfg, LakeTables.GOLD_DAILY_REVENUE).fqn() + " ("),
                "INSERT OVERWRITE 必须显式列列表");
    }

    @Test
    void migrationRenderingUsesRegisteredColumnFragments() {
        PlatformConfig cfg = cfg();
        String add = Migrations.addBatchRunsEpochColumns(cfg, Arrays.asList(
                Migrations.columnDefinition("started_ms"), Migrations.columnDefinition("finished_ms")));
        assertTrue(add.contains("started_ms BIGINT"));
        assertTrue(add.contains("finished_ms BIGINT"));
        assertTrue(add.contains(TableRef.of(cfg.catalogName(), LakeTables.OPS_SCHEMA,
                LakeTables.BATCH_RUNS).fqn()));
        assertFalse(SqlTemplate.hasUnresolved(add));

        assertTrue(Migrations.backfillBatchRunsEpoch(cfg).contains("unix_millis(started_at)"));
        assertTrue(Migrations.addSilverChannel(cfg).contains("ADD COLUMNS (channel STRING"));
        assertTrue(Migrations.backfillSilverChannel(cfg).contains("PMOD(HASH(user_id), 3)"));
        assertTrue(Migrations.addBronzeRunId(cfg).contains("ADD COLUMNS (run_id STRING"));
    }
}
