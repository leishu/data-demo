package com.demo.lake.common.catalog;

import com.demo.lake.common.config.PlatformConfig;
import lombok.experimental.UtilityClass;
import com.demo.lake.common.sql.SqlResources;
import com.demo.lake.common.sql.SqlScript;
import com.demo.lake.common.sql.SqlTemplate;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 湖仓分层表的 DDL 定义（Spark SQL / Iceberg 语法）。
 *
 * <p>分层：</p>
 * <ul>
 *   <li>ODS  bronze_orders        原始落地，只追加，按 event_time 天分区</li>
 *   <li>DWD  silver_orders        清洗 + 去重 + 派生字段，MERGE 增量更新</li>
 *   <li>ADS  gold_daily_revenue   按天/商户/品类的营收汇总，分区覆盖写</li>
 * </ul>
 *
 * <p>关键表属性说明：merge/update/delete 全部走 copy-on-write。
 * 读取侧（Trino）要处理 MoR 的 delete file，走 CoW 可以让 Spark 写、Trino 读
 * 这条链路零兼容问题（代价是写放大）。</p>
 *
 * <p>另有一个 ops 层（独立 namespace {@value #OPS_SCHEMA}）：{@code ops.batch_runs} 记录批作业
 * 的终态运行历史。放在业务库之外，就不会出现在 lake-service 的表浏览清单里。</p>
 *
 * <p>DDL 文本放在 classpath 的 {@code sql/ddl/} 下，由本类绑定配置参数后交给 Spark；
 * 表名常量仍由 Java 侧持有，单测会校验模板里的表名与常量一致。</p>
 */
@UtilityClass
public final class LakeTables {

    public static final String BRONZE_ORDERS = "bronze_orders";
    public static final String SILVER_ORDERS = "silver_orders";
    public static final String GOLD_DAILY_REVENUE = "gold_daily_revenue";

    /** 脱敏视图：分析角色（merchant_id 明文）与普通 viewer（全掩码）各一份，定义见 sql/view/ */
    public static final String BRONZE_ORDERS_MASKED = "bronze_orders_masked";
    public static final String BRONZE_ORDERS_RESTRICTED = "bronze_orders_restricted";
    public static final String SILVER_ORDERS_MASKED = "silver_orders_masked";
    public static final String SILVER_ORDERS_RESTRICTED = "silver_orders_restricted";

    /** 运维元数据 namespace：不进三层业务表清单 */
    public static final String OPS_SCHEMA = "ops";
    /** 批作业运行历史：一行一次运行的终态，由 BatchApp 写、lake-service 读 */
    public static final String BATCH_RUNS = "batch_runs";
    /** 数据质量检查结果：一行一次检查，quality 作业写、治理页读 */
    public static final String DQ_RESULTS = "dq_results";
    /** 写入级血缘：run_id -> table -> snapshot_id */
    public static final String LINEAGE_RUNS = "lineage_runs";
    /** 保留作业的执行前后审计事件 */
    public static final String RETENTION_AUDIT = "retention_audit";

    /** 建库 2 条 + 建表 4 条，顺序即执行顺序。 */
    public static List<String> all(PlatformConfig platformConfig) {
        List<String> out = new ArrayList<>();
        out.addAll(ddl(platformConfig, SqlResources.DDL_DATABASES, false));
        out.addAll(ddl(platformConfig, SqlResources.DDL_BRONZE, true));
        out.addAll(ddl(platformConfig, SqlResources.DDL_SILVER, true));
        out.addAll(ddl(platformConfig, SqlResources.DDL_GOLD, false));
        out.addAll(ddl(platformConfig, SqlResources.DDL_BATCH_RUNS, false));
        out.addAll(ddl(platformConfig, SqlResources.DDL_DQ_RESULTS, false));
        out.addAll(ddl(platformConfig, SqlResources.DDL_LINEAGE_RUNS, false));
        out.addAll(ddl(platformConfig, SqlResources.DDL_RETENTION_AUDIT, false));
        return out;
    }

    /**
     * 读取模板文件，绑定变量，生成 DDL 语句列表。
     * rowLevelWrites：这张表是否有行级写入（MERGE / UPDATE / DELETE）
     */
    private static List<String> ddl(PlatformConfig platformConfig, String resource, boolean rowLevelWrites) {
        List<String> out = new ArrayList<>();
        for (String stmt : SqlScript.split(SqlTemplate.renderResource(resource, vars(platformConfig, rowLevelWrites)))) {
            out.add(SqlScript.stripComments(stmt).trim());
        }
        return out;
    }

    private static Map<String, String> vars(PlatformConfig platformConfig, boolean rowLevelWrites) {
        Map<String, String> vars = new LinkedHashMap<>(baseVars(platformConfig));
        vars.put("table_properties", tableProperties(platformConfig, rowLevelWrites));
        return vars;
    }

    /** 各模板共用的基础变量；迁移模板也会用到。 */
    public static Map<String, String> baseVars(PlatformConfig platformConfig) {
        Map<String, String> vars = new LinkedHashMap<>();
        vars.put("catalog", platformConfig.catalogName());
        vars.put("database", platformConfig.database());
        vars.put("ops_schema", OPS_SCHEMA);
        vars.put("format_version", String.valueOf(platformConfig.formatVersion()));
        vars.put("write_mode", platformConfig.writeMode());
        return vars;
    }

    /**
     * 表属性片段也外置成 SQL 文件：有行级写入的表多 3 个 merge/update/delete mode，
     * 其他表只保留通用属性。片段首尾无分号，直接嵌进 {@code TBLPROPERTIES (...)}。
     */
    public static String tableProperties(PlatformConfig platformConfig, boolean rowLevelWrites) {
        String resource = rowLevelWrites ? SqlResources.PROPS_ROW_LEVEL : SqlResources.PROPS_PLAIN;
        return SqlTemplate.renderResource(resource, baseVars(platformConfig)).trim();
    }
}
