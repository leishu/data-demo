package com.demo.lake.common.semantic;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 语义层渲染：口径一致、方言有别、白名单挡住越权与注入 */
class SemanticSqlRendererTest {

    private static final String TABLE = "iceberg.lake.gold_daily_revenue";
    private static final MetricDefinition GMV = SemanticCatalog.get("gmv");

    @Test
    void trino方言渲染维度与过滤() {
        String sql = SemanticSqlRenderer.render(GMV, TABLE, List.of("dt", "merchant_id"),
                List.of(new SemanticFilter("dt", "gte", "2026-11-01"),
                        new SemanticFilter("merchant_id", "eq", "M'1")),
                100, SemanticSqlRenderer.Dialect.TRINO);

        assertTrue(sql.contains("\"dt\", \"merchant_id\", SUM(gmv) AS \"gmv\""));
        assertTrue(sql.contains("FROM " + TABLE));
        assertTrue(sql.contains("\"dt\" >= DATE '2026-11-01'"));
        assertTrue(sql.contains("\"merchant_id\" = 'M''1'"));
        assertTrue(sql.contains("GROUP BY \"dt\", \"merchant_id\""));
        assertTrue(sql.endsWith("LIMIT 100"));
    }

    @Test
    void spark方言只改标识符引号() {
        String spark = SemanticSqlRenderer.render(GMV, TABLE, List.of("dt"), List.of(), 50,
                SemanticSqlRenderer.Dialect.SPARK);
        String trino = SemanticSqlRenderer.render(GMV, TABLE, List.of("dt"), List.of(), 50,
                SemanticSqlRenderer.Dialect.TRINO);

        assertTrue(spark.contains("`dt`"));
        assertTrue(spark.contains("SUM(gmv) AS `gmv`"));
        assertTrue(trino.contains("\"dt\""));
        assertTrue(spark.replace("`", "\"").equals(trino));
    }

    @Test
    void 非白名单维度过滤操作与表名都被拒绝() {
        assertThrows(IllegalArgumentException.class, () -> SemanticSqlRenderer.render(
                GMV, TABLE, List.of("secret_column"), List.of(), 10, SemanticSqlRenderer.Dialect.TRINO));
        assertThrows(IllegalArgumentException.class, () -> SemanticSqlRenderer.render(
                GMV, TABLE, List.of(), List.of(new SemanticFilter("gmv", "gt", "100")), 10,
                SemanticSqlRenderer.Dialect.TRINO));
        assertThrows(IllegalArgumentException.class, () -> SemanticSqlRenderer.render(
                GMV, TABLE, List.of(), List.of(new SemanticFilter("dt", "like", "2026%")), 10,
                SemanticSqlRenderer.Dialect.TRINO));
        assertThrows(IllegalArgumentException.class, () -> SemanticSqlRenderer.render(
                GMV, "iceberg.lake.t;drop", List.of(), List.of(), 10, SemanticSqlRenderer.Dialect.TRINO));
    }

    @Test
    void 退款率口径用nullif避免除零() {
        String sql = SemanticSqlRenderer.render(SemanticCatalog.get("refund_rate"), TABLE, List.of("dt"),
                List.of(), 10, SemanticSqlRenderer.Dialect.TRINO);
        assertTrue(sql.contains("SUM(refund_amount) / NULLIF(SUM(gmv), 0)"));
    }
}
