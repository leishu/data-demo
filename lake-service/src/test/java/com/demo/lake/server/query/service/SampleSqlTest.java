package com.demo.lake.server.query.service;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.governance.MaskingCatalog;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 采样端点绝不能直连敏感基础表：角色到视图名的映射是最后一道闸。 */
class SampleSqlTest {

    @Test
    void 采敏感表时按角色切到脱敏视图() {
        String restricted = QueryService.sampleSql("iceberg", "lake", LakeTables.SILVER_ORDERS, 10,
                MaskingCatalog.Access.RESTRICTED);
        String analyst = QueryService.sampleSql("iceberg", "lake", LakeTables.SILVER_ORDERS, 10,
                MaskingCatalog.Access.ANALYST);
        String auditor = QueryService.sampleSql("iceberg", "lake", LakeTables.SILVER_ORDERS, 10,
                MaskingCatalog.Access.AUDITOR);

        assertTrue(restricted.contains("iceberg.lake." + LakeTables.SILVER_ORDERS_RESTRICTED), restricted);
        assertTrue(analyst.contains("iceberg.lake." + LakeTables.SILVER_ORDERS_MASKED), analyst);
        assertTrue(auditor.contains("iceberg.lake." + LakeTables.SILVER_ORDERS + " "), auditor);
        assertTrue(restricted.contains("LIMIT 10"));
        // 没带角色时按最严档处理，不能退回基础表
        assertEquals(restricted, QueryService.sampleSql("iceberg", "lake", LakeTables.SILVER_ORDERS, 10, null));
    }

    @Test
    void 非敏感表不被改写且标识符仍受校验() {
        String gold = QueryService.sampleSql("iceberg", "lake", LakeTables.GOLD_DAILY_REVENUE, 5,
                MaskingCatalog.Access.RESTRICTED);
        assertTrue(gold.contains("iceberg.lake." + LakeTables.GOLD_DAILY_REVENUE + " "), gold);
        assertThrows(RuntimeException.class,
                () -> QueryService.sampleSql("iceberg", "lake", "silver_orders;drop", 5,
                        MaskingCatalog.Access.RESTRICTED));
    }
}
