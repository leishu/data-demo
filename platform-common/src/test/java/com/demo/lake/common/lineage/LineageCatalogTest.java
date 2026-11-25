package com.demo.lake.common.lineage;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TransformSql;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlResources;
import com.demo.lake.common.sql.SqlScript;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 血缘声明的漂移守卫：改了转换 SQL 却忘了改 @lineage 注释，这里会挂 */
class LineageCatalogTest {

    private static final Pattern COLUMN_DEF = Pattern.compile(
            "^\\s{2,}([a-z][a-z0-9_]*)\\s+(STRING|DATE|TIMESTAMP|BIGINT|INT|BOOLEAN|DECIMAL)");

    private static Set<String> columnsOf(String ddlResource) {
        Set<String> columns = new HashSet<>();
        for (String line : SqlScript.readResource(ddlResource).split("\\R")) {
            Matcher m = COLUMN_DEF.matcher(line);
            if (m.find()) {
                columns.add(m.group(1));
            }
        }
        return columns;
    }

    private static String ddlOf(String table) {
        if (LakeTables.BRONZE_ORDERS.equals(table)) {
            return SqlResources.DDL_BRONZE;
        }
        if (LakeTables.SILVER_ORDERS.equals(table)) {
            return SqlResources.DDL_SILVER;
        }
        return SqlResources.DDL_GOLD;
    }

    @Test
    void 每个转换SQL都声明了一条表级血缘() {
        assertEquals(2, LineageCatalog.TABLE_EDGES.size());
        TableEdge merge = LineageCatalog.TABLE_EDGES.get(0);
        assertEquals(LakeTables.BRONZE_ORDERS, merge.getSource());
        assertEquals(LakeTables.SILVER_ORDERS, merge.getTarget());
        assertEquals("bronze_to_silver", merge.getTransform());

        TableEdge gold = LineageCatalog.TABLE_EDGES.get(1);
        assertEquals(LakeTables.SILVER_ORDERS, gold.getSource());
        assertEquals(LakeTables.GOLD_DAILY_REVENUE, gold.getTarget());
    }

    @Test
    void 列级血缘引用的表和列必须真实存在() {
        Set<String> pairs = new HashSet<>();
        for (TableEdge edge : LineageCatalog.TABLE_EDGES) {
            pairs.add(edge.getSource() + "->" + edge.getTarget());
        }
        assertFalse(LineageCatalog.COLUMN_EDGES.isEmpty());
        for (ColumnEdge edge : LineageCatalog.COLUMN_EDGES) {
            assertTrue(pairs.contains(edge.getSourceTable() + "->" + edge.getTargetTable()),
                    "列血缘缺少对应的表血缘: " + edge.getSourceTable() + "." + edge.getSourceColumn());
            assertTrue(columnsOf(ddlOf(edge.getSourceTable())).contains(edge.getSourceColumn()),
                    "源列在 DDL 里不存在: " + edge.getSourceTable() + "." + edge.getSourceColumn());
            assertTrue(columnsOf(ddlOf(edge.getTargetTable())).contains(edge.getTargetColumn()),
                    "目标列在 DDL 里不存在: " + edge.getTargetTable() + "." + edge.getTargetColumn());
        }
    }

    @Test
    void 可以从gold反查上游与列血缘() {
        assertEquals(Arrays.asList(LakeTables.SILVER_ORDERS, LakeTables.BRONZE_ORDERS),
                LineageCatalog.upstream(LakeTables.GOLD_DAILY_REVENUE));
        assertEquals(Arrays.asList(LakeTables.SILVER_ORDERS, LakeTables.GOLD_DAILY_REVENUE),
                LineageCatalog.downstream(LakeTables.BRONZE_ORDERS));
        assertTrue(LineageCatalog.columnsInto(LakeTables.GOLD_DAILY_REVENUE).stream()
                .anyMatch(e -> "amount".equals(e.getSourceColumn()) && "gmv".equals(e.getTargetColumn())));
        assertTrue(LineageCatalog.knownTable(LakeTables.GOLD_DAILY_REVENUE));
        assertFalse(LineageCatalog.knownTable("not_a_table"));
    }

    @Test
    void 写入表映射覆盖有数据变更的作业() {
        assertEquals(3, LineageCatalog.writtenTables(LakeJobs.INIT).size());
        assertEquals(List.of(LakeTables.BRONZE_ORDERS), LineageCatalog.writtenTables(LakeJobs.INGEST));
        assertEquals(Arrays.asList(LakeTables.SILVER_ORDERS, LakeTables.GOLD_DAILY_REVENUE),
                LineageCatalog.writtenTables(LakeJobs.TRANSFORM));
        assertTrue(LineageCatalog.writtenTables(LakeJobs.SHOW).isEmpty());
    }

    @Test
    void 声明是注释不会漏进执行的SQL() {
        PlatformConfig cfg = PlatformConfig.load();
        String merge = TransformSql.mergeBronzeIntoSilver(cfg, 7);
        String gold = TransformSql.rebuildGold(cfg);
        assertFalse(merge.contains("@lineage"), merge);
        assertFalse(gold.contains("@lineage"), gold);
        assertTrue(merge.startsWith("MERGE INTO"), merge);
        assertTrue(gold.startsWith("INSERT OVERWRITE"), gold);
    }

    /** 保留一份可维护的列清单，便于上面逐项核对而不依赖硬编码列表 */
    @Test
    void ddl列解析本身可用() {
        assertTrue(columnsOf(SqlResources.DDL_GOLD).containsAll(Arrays.asList(
                "dt", "merchant_id", "category", "order_cnt", "buyer_cnt",
                "gmv", "refund_amount", "net_revenue", "updated_at")));
        assertTrue(new ArrayList<>(columnsOf(SqlResources.DDL_BRONZE)).contains("run_id"));
    }
}
