package com.demo.lake.common.governance;

import com.demo.lake.common.catalog.LakeTables;
import lombok.experimental.UtilityClass;

import java.util.List;
import java.util.Map;

/**
 * 敏感列的脱敏口径与角色授权清单（单一事实源）。
 *
 * <p>和 {@link GovernanceCatalog} / {@link Sensitivity} 配套：资产目录说「这张表多敏感」，
 * 这里说「谁能看到哪一列」。实际掩码表达式在 {@code sql/view/} 与
 * {@code infra/trino/auth/rules.json} 里，两处都按这里的列名单维护，单测会卡住漂移。</p>
 *
 * <p>三档访问：</p>
 * <ul>
 *   <li>{@link Access#RESTRICTED}：普通 viewer，user_id / order_id / amount / merchant_id 全脱敏；</li>
 *   <li>{@link Access#ANALYST}：分析角色，merchant_id 明文，其余三列脱敏；</li>
 *   <li>{@link Access#AUDITOR}：审计/事件响应，走单独授权读基础表明文（对应查询原文保险箱的 legal_hold）。</li>
 * </ul>
 */
@UtilityClass
public final class MaskingCatalog {

    public enum Access {
        /** 默认档：所有敏感列（含 merchant_id）都掩码 */
        RESTRICTED,
        /** 分析档：merchant_id 明文，其余掩码 */
        ANALYST,
        /** 审计档：基础表明文，需 AUDITOR 角色 */
        AUDITOR
    }

    /** 任何档位下都必须脱敏的列；聚合表 gold 不含这些列 */
    public static final List<String> MASKED_COLUMNS = List.of("user_id", "order_id", "amount");
    /** 按角色放行的列：analyst 可见明文，viewer 哈希 */
    public static final List<String> ROLE_SCOPED_COLUMNS = List.of("merchant_id");

    private static final Map<String, String> ANALYST_VIEWS = Map.of(
            LakeTables.BRONZE_ORDERS, LakeTables.BRONZE_ORDERS_MASKED,
            LakeTables.SILVER_ORDERS, LakeTables.SILVER_ORDERS_MASKED);
    private static final Map<String, String> RESTRICTED_VIEWS = Map.of(
            LakeTables.BRONZE_ORDERS, LakeTables.BRONZE_ORDERS_RESTRICTED,
            LakeTables.SILVER_ORDERS, LakeTables.SILVER_ORDERS_RESTRICTED);

    public static boolean hasView(String table) {
        return ANALYST_VIEWS.containsKey(table);
    }

    /** 按角色解析查询目标：审计档读基础表，其余档读对应视图。非敏感表原样返回。 */
    public static String targetFor(String table, Access access) {
        if (!hasView(table)) {
            return table;
        }
        if (access == Access.AUDITOR) {
            return table;
        }
        return access == Access.ANALYST ? ANALYST_VIEWS.get(table) : RESTRICTED_VIEWS.get(table);
    }

    public static String analystView(String table) {
        return ANALYST_VIEWS.get(table);
    }

    public static String restrictedView(String table) {
        return RESTRICTED_VIEWS.get(table);
    }

    public static boolean isMaskedColumn(String column) {
        return column != null && MASKED_COLUMNS.contains(column.toLowerCase(java.util.Locale.ROOT));
    }
}
