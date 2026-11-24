package com.demo.lake.common.catalog;

import com.demo.lake.common.config.PlatformConfig;
import lombok.experimental.UtilityClass;
import com.demo.lake.common.sql.SqlResources;
import com.demo.lake.common.sql.SqlScript;
import com.demo.lake.common.sql.SqlTemplate;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 分层转换 SQL（Spark SQL / Iceberg 语法）。
 * 抽到 common 里，Spark 作业和单测共用同一份 SQL，避免"测试用的 SQL 和线上跑的不一样"。
 * SQL 文本在 classpath 的 {@code sql/transform/} 下，这里只负责绑定表名和天数。
 */
@UtilityClass
public final class TransformSql {

    /**
     * ODS -&gt; DWD：按 order_id 取最新事件做 upsert。
     * 只扫最近 days 天的分区，靠 Iceberg 的分区裁剪控制扫描量。
     */
    public static String mergeBronzeIntoSilver(PlatformConfig cfg, int days) {
        Map<String, String> vars = vars(cfg);
        vars.put("days", String.valueOf(days));
        return one(SqlResources.TRANSFORM_MERGE, vars);
    }

    /**
     * DWD -&gt; ADS：按天/商户/品类重算汇总。
     * INSERT OVERWRITE 在 Iceberg 里是动态分区覆盖，只替换出现的分区；
     * 若要按天增量，在 WHERE 里加 dt 条件即可，例如 dt = current_date - 1。
     *
     * <p>注意：buyer_cnt 是 (dt, 商户, 品类) 粒度下的去重用户数，跨行相加得到的是“人次”，
     * 不是全站去重买家数。</p>
     */
    public static String rebuildGold(PlatformConfig cfg) {
        return one(SqlResources.TRANSFORM_GOLD, vars(cfg));
    }

    private static Map<String, String> vars(PlatformConfig cfg) {
        Map<String, String> vars = new LinkedHashMap<>(LakeTables.baseVars(cfg));
        vars.put("bronze_table", TableRef.of(cfg, LakeTables.BRONZE_ORDERS).fqn());
        vars.put("silver_table", TableRef.of(cfg, LakeTables.SILVER_ORDERS).fqn());
        vars.put("gold_table", TableRef.of(cfg, LakeTables.GOLD_DAILY_REVENUE).fqn());
        return vars;
    }

    private static String one(String resource, Map<String, String> vars) {
        List<String> statements = SqlScript.split(SqlTemplate.renderResource(resource, vars));
        if (statements.size() != 1) {
            throw new IllegalStateException("转换 SQL 模板应只有一条语句: " + resource);
        }
        // 去掉模板头注释，返回纯 SQL：注释留在 .sql 文件里给人读，不进 spark.sql()
        return SqlScript.stripComments(statements.get(0)).trim();
    }
}
