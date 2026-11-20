package com.demo.lake.common.semantic;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 把「指标 + 维度 + 过滤」渲染成 SQL。多引擎差异只体现在标识符引号上：
 * Trino 用双引号，Spark 用反引号；口径表达式与过滤语义两处完全一致。
 *
 * <p>所有标识符都来自指标目录的白名单，过滤值按类型转成日期/数字/转义字符串，
 * 所以这个接口不会变成另一个 SQL 注入入口。</p>
 */
public final class SemanticSqlRenderer {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final Map<String, String> SQL_OPS = Map.of(
            "eq", "=", "neq", "<>", "gt", ">", "gte", ">=", "lt", "<", "lte", "<=");

    public enum Dialect {
        TRINO {
            @Override
            String ident(String name) {
                return "\"" + name + "\"";
            }
        },
        SPARK {
            @Override
            String ident(String name) {
                return "`" + name + "`";
            }
        };

        abstract String ident(String name);
    }

    private SemanticSqlRenderer() {
    }

    public static String render(MetricDefinition metric, String tableFqn, List<String> dimensions,
                                List<SemanticFilter> filters, int limit, Dialect dialect) {
        if (metric == null || tableFqn == null || dialect == null) {
            throw new IllegalArgumentException("metric / tableFqn / dialect 不能为空");
        }
        for (String part : tableFqn.split("\\.")) {
            if (!IDENTIFIER.matcher(part).matches()) {
                throw new IllegalArgumentException("非法表名: " + tableFqn);
            }
        }

        Set<String> allowed = new LinkedHashSet<>(metric.getDimensions());
        List<String> dims = new ArrayList<>();
        if (dimensions != null) {
            for (String dim : dimensions) {
                if (dim == null || !allowed.contains(dim)) {
                    throw new IllegalArgumentException("指标 " + metric.getName() + " 不支持维度: " + dim);
                }
                if (!dims.contains(dim)) {
                    dims.add(dim);
                }
            }
        }

        StringBuilder sql = new StringBuilder("select ");
        for (int i = 0; i < dims.size(); i++) {
            sql.append(dialect.ident(dims.get(i))).append(", ");
        }
        sql.append(metric.getMeasure()).append(" as ").append(dialect.ident(metric.getName()));
        sql.append(" from ").append(tableFqn);

        if (filters != null && !filters.isEmpty()) {
            sql.append(" where ");
            for (int i = 0; i < filters.size(); i++) {
                if (i > 0) {
                    sql.append(" and ");
                }
                sql.append(condition(filters.get(i), allowed, dialect));
            }
        }
        if (!dims.isEmpty()) {
            sql.append(" group by ");
            for (int i = 0; i < dims.size(); i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(dialect.ident(dims.get(i)));
            }
            sql.append(" order by ").append(dialect.ident(dims.get(0))).append(" desc");
        }
        sql.append(" limit ").append(Math.max(1, Math.min(limit, 1000)));
        return sql.toString();
    }

    private static String condition(SemanticFilter filter, Set<String> allowed, Dialect dialect) {
        if (filter == null || filter.getField() == null || !allowed.contains(filter.getField())) {
            throw new IllegalArgumentException("过滤字段不在指标维度白名单里: " +
                    (filter == null ? "null" : filter.getField()));
        }
        String op = filter.getOp() == null ? "" : filter.getOp().toLowerCase(Locale.ROOT);
        String sqlOp = SQL_OPS.get(op);
        if (sqlOp == null) {
            throw new IllegalArgumentException("不支持的过滤操作: " + filter.getOp());
        }
        return dialect.ident(filter.getField()) + " " + sqlOp + " " + literal(filter.getField(), filter.getValue());
    }

    private static String literal(String field, String value) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("过滤值不能为空: " + field);
        }
        if ("dt".equals(field)) {
            if (!DATE.matcher(value).matches()) {
                throw new IllegalArgumentException("dt 过滤值必须是 yyyy-MM-dd: " + value);
            }
            return "date '" + value + "'";
        }
        try {
            new BigDecimal(value);
            return value; // 数值字面量，已经校验过
        } catch (NumberFormatException ignored) {
            return "'" + value.replace("'", "''") + "'";
        }
    }
}
