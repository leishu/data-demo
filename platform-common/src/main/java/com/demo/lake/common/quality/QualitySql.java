package com.demo.lake.common.quality;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlScript;
import com.demo.lake.common.sql.SqlTemplate;
import lombok.experimental.UtilityClass;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 把质量检查模板绑上表名，返回可直接交给 spark.sql() 的单条语句 */
@UtilityClass
public final class QualitySql {

    public static String render(QualityCheck check, PlatformConfig platformConfig) {
        Map<String, String> vars = new LinkedHashMap<>(LakeTables.baseVars(platformConfig));
        vars.put("silver_table", TableRef.of(platformConfig, LakeTables.SILVER_ORDERS).fqn());
        vars.put("gold_table", TableRef.of(platformConfig, LakeTables.GOLD_DAILY_REVENUE).fqn());
        List<String> statements = SqlScript.split(SqlTemplate.renderResource(check.resource(), vars));
        if (statements.size() != 1) {
            throw new IllegalStateException("质量检查模板应只有一条语句: " + check.resource());
        }
        return SqlScript.stripComments(statements.get(0)).trim();
    }
}
