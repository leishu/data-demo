package com.demo.lake.common.quality;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlTemplate;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 检查项注册表 + SQL 模板守卫：漏登记、占位符写错、表名漂移都在这里挂掉 */
class QualityChecksTest {

    private static PlatformConfig cfg() {
        return PlatformConfig.load();
    }

    @Test
    void 六项检查登记完整且名字唯一() {
        Set<String> names = new HashSet<>();
        for (QualityCheck check : QualityChecks.ALL) {
            assertTrue(names.add(check.name()), "检查名重复: " + check.name());
            assertNotNull(check.resource());
            assertNotNull(check.label());
            assertTrue(List.of("silver", "gold").contains(check.source()));
        }
        assertEquals(6, names.size());
        assertEquals(QualitySeverity.ERROR, QualityChecks.GOLD_REFUND_RATE.severity());
        assertEquals(QualitySeverity.WARN, QualityChecks.GOLD_ROW_COUNT.severity());
        assertTrue(QualitySeverity.ERROR.failsJob());
        assertFalse(QualitySeverity.WARN.failsJob());
    }

    @Test
    void 按名字查找() {
        assertNotNull(QualityChecks.get("silver_unique"));
        assertNull(QualityChecks.get("not_a_check"));
    }

    @Test
    void 每个模板渲染成单条无残留占位符的SQL() {
        PlatformConfig cfg = cfg();
        for (QualityCheck check : QualityChecks.ALL) {
            String sql = QualitySql.render(check, cfg);
            assertFalse(sql.isBlank(), check.name());
            assertFalse(SqlTemplate.hasUnresolved(sql), check.name() + " 残留占位符");
            assertFalse(sql.contains(";"), check.name() + " 应只有一条语句");
            if ("silver".equals(check.source())) {
                assertTrue(sql.contains(TableRef.of(cfg, LakeTables.SILVER_ORDERS).fqn()), check.name());
            } else {
                assertTrue(sql.contains(TableRef.of(cfg, LakeTables.GOLD_DAILY_REVENUE).fqn()), check.name());
            }
        }
    }
}
