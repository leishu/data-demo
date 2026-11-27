package com.demo.lake.common.catalog;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlResources;
import com.demo.lake.common.sql.SqlScript;
import com.demo.lake.common.sql.SqlTemplate;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;

/**
 * 脱敏视图的 DDL 装载器：SQL 全外置在 {@code sql/view/}，由 Trino 执行（不是 Spark DDL）。
 *
 * <p>和 {@link LakeTables} 的区别只在执行引擎：LakeTables 产出 Spark SQL 建表语句，这里产出
 * Trino SQL 的 {@code CREATE OR REPLACE VIEW}。视图刻意不写物理表，查询时始终读基础表的最新快照，
 * 所以 ingest / transform 之后不需要重建视图。</p>
 */
@UtilityClass
public final class LakeViews {

    /** 顺序即创建顺序：先 analyst 视图，再受限视图；二者列顺序一致，调用方按角色切换无感。 */
    private static final List<String> TEMPLATES = List.of(
            SqlResources.VIEW_BRONZE_MASKED,
            SqlResources.VIEW_BRONZE_RESTRICTED,
            SqlResources.VIEW_SILVER_MASKED,
            SqlResources.VIEW_SILVER_RESTRICTED);

    public static List<String> all(PlatformConfig platformConfig) {
        List<String> out = new ArrayList<>();
        for (String resource : TEMPLATES) {
            String rendered = SqlTemplate.renderResource(resource, LakeTables.baseVars(platformConfig));
            for (String stmt : SqlScript.split(rendered)) {
                String sql = SqlScript.stripComments(stmt).trim();
                if (!sql.isEmpty()) {
                    out.add(sql);
                }
            }
        }
        return out;
    }
}
