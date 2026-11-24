package com.demo.lake.common.governance;

import com.demo.lake.common.catalog.LakeTables;
import lombok.experimental.UtilityClass;

import java.util.List;

/**
 * 资产目录：每张湖表的责任人 / 业务域 / 分层 / 敏感级别 / 保留天数 / 新鲜度 SLA 只在这里维护。
 *
 * <p>刻意用 Java 常量而不是 YAML：platform-common 被 spark-batch / lake-adhoc / lake-service 共用，
 * 引一份 YAML 解析就要多带一个依赖，且“目录写错”会拖到运行时才暴露。这里和
 * {@link com.demo.lake.common.semantic.SemanticCatalog}、{@link com.demo.lake.common.batch.LakeJobs}
 * 一样是单一事实源，单测会把表名、重复项和字段约束全部卡住。</p>
 */
@UtilityClass
public final class GovernanceCatalog {

    public static final List<AssetDefinition> ASSETS = List.of(
            new AssetDefinition(LakeTables.BRONZE_ORDERS, "数据平台组", "交易", "ODS",
                    Sensitivity.INTERNAL, 90, 26, "订单原始落地，只追加，含约 3% 重复与 10% 迟到数据"),
            new AssetDefinition(LakeTables.SILVER_ORDERS, "数据平台组", "交易", "DWD",
                    Sensitivity.SENSITIVE, 365, 26, "按 order_id 去重后的事实表，一行一单"),
            new AssetDefinition(LakeTables.GOLD_DAILY_REVENUE, "经营分析组", "经营分析", "ADS",
                    Sensitivity.INTERNAL, 730, 26, "按天 × 商户 × 品类汇总的 GMV / 退款 / 净收入"));

    public static AssetDefinition get(String table) {
        for (AssetDefinition asset : ASSETS) {
            if (asset.getTable().equals(table)) {
                return asset;
            }
        }
        return null;
    }
}
