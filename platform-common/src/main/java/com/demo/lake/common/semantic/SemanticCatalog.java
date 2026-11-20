package com.demo.lake.common.semantic;

import com.demo.lake.common.catalog.LakeTables;

import java.util.List;

/** 指标目录：所有口径只在这里维护，服务与 UI 都从它读取 */
public final class SemanticCatalog {

    private static final List<String> GOLD_DIMENSIONS = List.of("dt", "merchant_id", "category");

    public static final List<MetricDefinition> METRICS = List.of(
            new MetricDefinition("gmv", "成交总额", "有效订单的 GMV 之和",
                    LakeTables.GOLD_DAILY_REVENUE, "sum(gmv)", GOLD_DIMENSIONS),
            new MetricDefinition("orders", "有效订单数", "计入口径的订单数之和",
                    LakeTables.GOLD_DAILY_REVENUE, "sum(order_cnt)", GOLD_DIMENSIONS),
            new MetricDefinition("buyers", "下单人数", "按天/商户/品类统计的下单人数之和（跨维度不可相加）",
                    LakeTables.GOLD_DAILY_REVENUE, "sum(buyer_cnt)", GOLD_DIMENSIONS),
            new MetricDefinition("net_revenue", "净收入", "GMV 扣掉退款后的金额之和",
                    LakeTables.GOLD_DAILY_REVENUE, "sum(net_revenue)", GOLD_DIMENSIONS),
            new MetricDefinition("refund_rate", "退款率", "退款金额 / GMV",
                    LakeTables.GOLD_DAILY_REVENUE, "sum(refund_amount) / nullif(sum(gmv), 0)", GOLD_DIMENSIONS));

    private SemanticCatalog() {
    }

    public static MetricDefinition get(String name) {
        return METRICS.stream().filter(m -> m.getName().equals(name)).findFirst().orElse(null);
    }
}
