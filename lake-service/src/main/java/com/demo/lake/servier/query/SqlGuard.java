package com.demo.lake.servier.query;

import java.util.Locale;
import java.util.Set;

/**
 * 即席查询的 SQL 守卫。
 *
 * <p>对外暴露一个「能跑任意 SQL」的 HTTP 接口风险极高，这里做三件事：
 * 只允许单条语句、去掉注释后再判断首关键字、默认只放行只读语句（白名单，不是黑名单）。</p>
 */
public final class SqlGuard {

    private static final Set<String> READ_ONLY_KEYWORDS =
            Set.of("select", "with", "show", "describe", "desc", "explain");

    private SqlGuard() {
    }

    /**
     * @param rawSql      调用方传入的 SQL
     * @param allowWrites 是否允许 DDL/DML
     * @return 去掉注释与结尾分号、可直接执行的 SQL
     */
    public static String requireReadOnly(String rawSql, boolean allowWrites) {
        if (rawSql == null || rawSql.isBlank()) {
            throw new InvalidQueryException("sql 不能为空");
        }
        String sql = stripComments(rawSql).trim();
        while (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        if (sql.isEmpty()) {
            throw new InvalidQueryException("sql 不能为空");
        }
        if (sql.indexOf(';') >= 0) {
            throw new InvalidQueryException("只允许单条语句，检测到多个分号");
        }
        if (!allowWrites && !READ_ONLY_KEYWORDS.contains(firstKeyword(sql))) {
            throw new InvalidQueryException(
                    "当前实例只放行只读语句（select/with/show/describe/explain）；需要写入请把 lake.query.allow-writes 置为 true");
        }
        return sql;
    }

    static String firstKeyword(String sql) {
        int i = 0;
        while (i < sql.length() && Character.isWhitespace(sql.charAt(i))) {
            i++;
        }
        int start = i;
        while (i < sql.length() && (Character.isLetter(sql.charAt(i)) || sql.charAt(i) == '_')) {
            i++;
        }
        return sql.substring(start, i).toLowerCase(Locale.ROOT);
    }

    static String stripComments(String sql) {
        // 块注释 + 行注释；字符串字面量里的 "--" 会误删，但即席查询里可接受
        String noBlock = sql.replaceAll("(?s)/\\*.*?\\*/", " ");
        return noBlock.replaceAll("(?m)--[^\\n]*", " ");
    }
}
