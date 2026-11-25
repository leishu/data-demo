package com.demo.lake.common.quality;

import com.demo.lake.common.sql.SqlResources;
import lombok.experimental.UtilityClass;

import java.util.List;

/**
 * 质量检查项注册表：所有检查只在这里登记，作业执行、结果展示共用同一份名字与级别。
 */
@UtilityClass
public final class QualityChecks {

    public static final QualityCheck SILVER_UNIQUE = new QualityCheck(
            "silver_unique", "订单号唯一性", QualitySeverity.ERROR, "silver",
            SqlResources.QUALITY_SILVER_UNIQUE);
    public static final QualityCheck SILVER_NOT_NULL = new QualityCheck(
            "silver_not_null", "关键字段非空", QualitySeverity.ERROR, "silver",
            SqlResources.QUALITY_SILVER_NOT_NULL);
    public static final QualityCheck SILVER_AMOUNT_RANGE = new QualityCheck(
            "silver_amount_range", "订单金额范围", QualitySeverity.ERROR, "silver",
            SqlResources.QUALITY_SILVER_AMOUNT_RANGE);
    public static final QualityCheck GOLD_REFUND_RATE = new QualityCheck(
            "gold_refund_rate", "退款率 0~1", QualitySeverity.ERROR, "gold",
            SqlResources.QUALITY_GOLD_REFUND_RATE);
    public static final QualityCheck GOLD_NET_REVENUE = new QualityCheck(
            "gold_net_revenue", "净收入一致性", QualitySeverity.ERROR, "gold",
            SqlResources.QUALITY_GOLD_NET_REVENUE);
    public static final QualityCheck GOLD_ROW_COUNT = new QualityCheck(
            "gold_row_count", "行数波动(相邻天)", QualitySeverity.WARN, "gold",
            SqlResources.QUALITY_GOLD_ROW_COUNT);

    public static final List<QualityCheck> ALL = List.of(
            SILVER_UNIQUE, SILVER_NOT_NULL, SILVER_AMOUNT_RANGE,
            GOLD_REFUND_RATE, GOLD_NET_REVENUE, GOLD_ROW_COUNT);

    public static QualityCheck get(String name) {
        for (QualityCheck check : ALL) {
            if (check.getName().equals(name)) {
                return check;
            }
        }
        return null;
    }
}
