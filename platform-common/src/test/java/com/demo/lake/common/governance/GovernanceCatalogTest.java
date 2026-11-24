package com.demo.lake.common.governance;

import com.demo.lake.common.catalog.LakeTables;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 目录是常量，写错的代价是页面给出错误的责任人/级别，所以在这里把字段全部卡死 */
class GovernanceCatalogTest {

    private static final Set<String> KNOWN_TABLES = Set.of(
            LakeTables.BRONZE_ORDERS, LakeTables.SILVER_ORDERS, LakeTables.GOLD_DAILY_REVENUE);

    @Test
    void 每张湖表都有且仅有一条档案() {
        List<AssetDefinition> assets = GovernanceCatalog.ASSETS;
        Set<String> names = new HashSet<>();
        for (AssetDefinition asset : assets) {
            assertTrue(KNOWN_TABLES.contains(asset.getTable()), "目录出现未知湖表: " + asset.getTable());
            assertTrue(names.add(asset.getTable()), "目录重复登记: " + asset.getTable());
        }
        assertEquals(KNOWN_TABLES, names);
    }

    @Test
    void 分层和敏感级别合法且必填字段齐全() {
        for (AssetDefinition asset : GovernanceCatalog.ASSETS) {
            assertTrue(List.of("ODS", "DWD", "ADS").contains(asset.getLayer()),
                    "分层只能是 ODS/DWD/ADS: " + asset.getTable());
            assertNotNull(asset.getSensitivity(), asset.getTable());
            assertTrue(asset.getOwner().length() > 1, asset.getTable());
            assertTrue(asset.getDomain().length() > 1, asset.getTable());
            assertTrue(asset.getDescription().length() > 1, asset.getTable());
            assertTrue(asset.getRetentionDays() > 0, asset.getTable());
            assertTrue(asset.getFreshnessSlaHours() > 0, asset.getTable());
        }
    }

    @Test
    void 敏感级别按四级递增且标签可读() {
        assertEquals(4, Sensitivity.values().length);
        int last = 0;
        for (Sensitivity sensitivity : Sensitivity.values()) {
            assertTrue(sensitivity.level() > last, "敏感级别必须递增: " + sensitivity);
            assertTrue(sensitivity.label().length() > 0);
            last = sensitivity.level();
        }
    }

    @Test
    void 按表名查找未登记返回空() {
        AssetDefinition silver = GovernanceCatalog.get(LakeTables.SILVER_ORDERS);
        assertNotNull(silver);
        assertEquals(Sensitivity.SENSITIVE, silver.getSensitivity());
        assertNull(GovernanceCatalog.get("not_a_lake_table"));
    }

    @Test
    void 非法档案在构造阶段就被拒绝() {
        assertThrows(IllegalArgumentException.class, () -> new AssetDefinition(
                LakeTables.BRONZE_ORDERS, "  ", "交易", "ODS", Sensitivity.INTERNAL, 90, 26, "描述"));
        assertThrows(IllegalArgumentException.class, () -> new AssetDefinition(
                LakeTables.BRONZE_ORDERS, "数据平台组", "交易", "ODS", Sensitivity.INTERNAL, 0, 26, "描述"));
        assertThrows(IllegalArgumentException.class, () -> new AssetDefinition(
                LakeTables.BRONZE_ORDERS, "数据平台组", "交易", "ODS", null, 90, 26, "描述"));
    }
}
