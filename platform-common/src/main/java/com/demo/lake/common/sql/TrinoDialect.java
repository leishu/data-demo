package com.demo.lake.common.sql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 最小限度的 Trino -&gt; Spark SQL 方言适配，只服务于 {@code --job verify-sql}
 * （没装 Trino 时用 Spark 校验脚本能不能跑）。
 *
 * <p>不做通用方言翻译，只覆盖本项目脚本里出现的两种差异：</p>
 * <ul>
 *   <li>{@code date_add('day', -6, current_date)} -&gt; {@code date_sub(current_date, 6)}</li>
 *   <li>{@code INTERVAL '1' DAY} -&gt; {@code INTERVAL 1 DAYS}</li>
 * </ul>
 */
public final class TrinoDialect {

    private static final Pattern DATE_ADD = Pattern.compile(
            "date_add\\(\\s*'day'\\s*,\\s*(-?\\d+)\\s*,\\s*([A-Za-z_][A-Za-z0-9_.]*(?:\\(\\))?)\\s*\\)",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern QUOTED_INTERVAL = Pattern.compile(
            "INTERVAL\\s+'(-?\\d+)'\\s+(DAY|DAYS|HOUR|HOURS|MONTH|MONTHS|YEAR|YEARS|MINUTE|MINUTES)",
            Pattern.CASE_INSENSITIVE);

    private TrinoDialect() {
    }

    public static String toSpark(String sql) {
        String result = rewriteDateAdd(sql);
        return rewriteInterval(result);
    }

    private static String rewriteDateAdd(String sql) {
        Matcher m = DATE_ADD.matcher(sql);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            int days = Integer.parseInt(m.group(1));
            String expr = m.group(2);
            String replacement = days < 0
                    ? "date_sub(" + expr + ", " + (-days) + ")"
                    : "date_add(" + expr + ", " + days + ")";
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static String rewriteInterval(String sql) {
        Matcher m = QUOTED_INTERVAL.matcher(sql);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String unit = m.group(2).toUpperCase();
            if (!unit.endsWith("S")) {
                unit = unit + "S";
            }
            m.appendReplacement(sb, Matcher.quoteReplacement("INTERVAL " + m.group(1) + " " + unit));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
