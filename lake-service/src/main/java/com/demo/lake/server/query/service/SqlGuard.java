package com.demo.lake.server.query.service;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 即席查询的 SQL 守卫。
 *
 * <p>对外暴露一个「能跑任意 SQL」的 HTTP 接口风险极高，这里做三件事：
 * 只允许单条语句、去掉注释后再判断首关键字、默认只放行只读语句（白名单，不是黑名单）。</p>
 *
 * <p>特别注意 {@code EXPLAIN ANALYZE}：它会把内层语句真实执行一遍，而 Trino JDBC 的
 * {@code setReadOnly} 只是驱动里的一个标记，并不会在服务端拦截写入。所以只读模式下
 * 必须看穿 EXPLAIN 的内层语句，否则 {@code EXPLAIN ANALYZE INSERT ...} 就能绕过守卫。</p>
 */
public final class SqlGuard {

    private static final Set<String> READ_ONLY_KEYWORDS =
            Set.of("select", "with", "show", "describe", "desc", "explain");
    /** EXPLAIN 可以嵌套（explain analyze explain ...），给个上限防止异常输入把栈打穿 */
    private static final int MAX_EXPLAIN_DEPTH = 5;
    private static final Pattern ANALYZE_WORD = Pattern.compile("(?i)\\banalyze\\b");

    private SqlGuard() {
    }

    /**
     * @param rawSql      调用方传入的 SQL
     * @param allowWrites 是否允许 DDL/DML
     * @return 去掉注释与结尾分号、可直接执行的 SQL
     */
    public static String requireReadOnly(String rawSql, boolean allowWrites) {
        return requireReadOnly(rawSql, allowWrites, 0);
    }

    private static String requireReadOnly(String rawSql, boolean allowWrites, int depth) {
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
        String keyword = firstKeyword(sql);
        if (!allowWrites && !READ_ONLY_KEYWORDS.contains(keyword)) {
            throw new InvalidQueryException(
                    "当前实例只放行只读语句（select/with/show/describe/explain）；需要写入请把 lake.query.allow-writes 置为 true");
        }
        if (!allowWrites && "explain".equals(keyword)) {
            String inner = analyzedStatement(sql);
            if (inner != null) {
                // ANALYZE 会真实执行内层语句：把内层当成一条独立 SQL 重新按只读校验
                if (inner.isEmpty()) {
                    throw new InvalidQueryException("EXPLAIN ANALYZE 后面缺少要执行的语句");
                }
                if (depth >= MAX_EXPLAIN_DEPTH) {
                    throw new InvalidQueryException("EXPLAIN 嵌套层数过多");
                }
                requireReadOnly(inner, false, depth + 1);
            }
        }
        return sql;
    }

    /**
     * 解析 EXPLAIN 头：出现 ANALYZE（含 {@code EXPLAIN (ANALYZE) ...} 写法）时返回内层语句，
     * 否则返回 null。这里只做「够用」的扫描，语法错误仍交给 Trino 报。
     */
    static String analyzedStatement(String sql) {
        int i = skipSpaces(sql, 0);
        // 跳过 explain 本身
        while (i < sql.length() && (Character.isLetter(sql.charAt(i)) || sql.charAt(i) == '_')) {
            i++;
        }
        boolean analyzed = false;
        while (true) {
            i = skipSpaces(sql, i);
            if (i >= sql.length()) {
                break;
            }
            if (sql.charAt(i) == '(') {
                int end = matchingParen(sql, i);
                if (end < 0) {
                    // 括号没闭合：让 Trino 去报语法错误
                    return null;
                }
                if (ANALYZE_WORD.matcher(sql.substring(i + 1, end)).find()) {
                    analyzed = true;
                }
                i = end + 1;
                continue;
            }
            int start = i;
            int end = i;
            while (end < sql.length() && (Character.isLetter(sql.charAt(end)) || sql.charAt(end) == '_')) {
                end++;
            }
            if (start == end) {
                break; // 不是关键字（数字/引号等），内层语句从这里开始
            }
            String word = sql.substring(start, end).toLowerCase(Locale.ROOT);
            if ("analyze".equals(word)) {
                analyzed = true;
                i = end;
                continue;
            }
            if ("verbose".equals(word)) {
                i = end;
                continue;
            }
            // 内层语句的第一个关键字：留在原地，别把它吞掉
            i = start;
            break;
        }
        return analyzed ? sql.substring(i).trim() : null;
    }

    private static int skipSpaces(String sql, int from) {
        int i = from;
        while (i < sql.length() && Character.isWhitespace(sql.charAt(i))) {
            i++;
        }
        return i;
    }

    private static int matchingParen(String sql, int open) {
        int depth = 0;
        for (int i = open; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            if (ch == '(') {
                depth++;
            } else if (ch == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
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

    /** 去掉行注释与块注释，但保留字符串字面量/带引号标识符里长得像注释标记的文本。 */
    static String stripComments(String sql) {
        StringBuilder out = new StringBuilder(sql.length());
        char quote = 0;
        int i = 0;
        while (i < sql.length()) {
            char ch = sql.charAt(i);
            if (quote != 0) {
                out.append(ch);
                if (ch == quote) {
                    // SQL 里用两个连续引号转义（'a''b' / "a""b" / `a``b`）
                    if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
                        out.append(quote);
                        i += 2;
                        continue;
                    }
                    quote = 0;
                }
                i++;
                continue;
            }
            if (ch == '\'' || ch == '"' || ch == '`') {
                quote = ch;
                out.append(ch);
                i++;
                continue;
            }
            if (ch == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                while (i < sql.length() && sql.charAt(i) != '\n') {
                    i++;
                }
                out.append(' ');
                continue;
            }
            if (ch == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                i += 2;
                while (i + 1 < sql.length() && !(sql.charAt(i) == '*' && sql.charAt(i + 1) == '/')) {
                    i++;
                }
                i = Math.min(sql.length(), i + 2);
                out.append(' ');
                continue;
            }
            out.append(ch);
            i++;
        }
        return out.toString();
    }
}
