package com.demo.lake.common.governance;

import com.demo.lake.common.catalog.LakeTables;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 脱敏目录是服务采样与 Trino 授权的共同依据；角色映射写错会让 viewer 看到明文。 */
class MaskingCatalogTest {

    @Test
    void 三档访问解析到不同查询目标() {
        for (String table : new String[]{LakeTables.BRONZE_ORDERS, LakeTables.SILVER_ORDERS}) {
            assertTrue(MaskingCatalog.hasView(table));
            assertEquals(table, MaskingCatalog.targetFor(table, MaskingCatalog.Access.AUDITOR));
            assertEquals(MaskingCatalog.analystView(table),
                    MaskingCatalog.targetFor(table, MaskingCatalog.Access.ANALYST));
            assertEquals(MaskingCatalog.restrictedView(table),
                    MaskingCatalog.targetFor(table, MaskingCatalog.Access.RESTRICTED));
            assertFalse(MaskingCatalog.analystView(table).equals(MaskingCatalog.restrictedView(table)));
        }
    }

    @Test
    void 非敏感表不替换查询目标() {
        assertFalse(MaskingCatalog.hasView(LakeTables.GOLD_DAILY_REVENUE));
        for (MaskingCatalog.Access access : MaskingCatalog.Access.values()) {
            assertEquals(LakeTables.GOLD_DAILY_REVENUE,
                    MaskingCatalog.targetFor(LakeTables.GOLD_DAILY_REVENUE, access));
        }
    }

    @Test
    void 脱敏清单与敏感级别对齐() {
        assertEquals(3, MaskingCatalog.MASKED_COLUMNS.size());
        assertTrue(MaskingCatalog.MASKED_COLUMNS.containsAll(java.util.List.of("user_id", "order_id", "amount")));
        assertTrue(MaskingCatalog.isMaskedColumn("USER_ID"));
        assertFalse(MaskingCatalog.isMaskedColumn("merchant_id"));
        assertEquals(Sensitivity.SENSITIVE,
                GovernanceCatalog.get(LakeTables.SILVER_ORDERS).getSensitivity());
    }
}
