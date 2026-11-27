package com.demo.lake.common.sql;

import lombok.experimental.UtilityClass;

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
@UtilityClass
public final class TrinoDialect {

    private static final Pattern DATE_ADD = Pattern.compile(
            "date_add\\(\\s*'day'\\s*,\\s*(-?\\d+)\\s*,\\s*([A-Za-z_][A-Za-z0-9_.]*(?:\\(\\))?)\\s*\\)",
            Pattern.CASE_INSENSITIVE);

    private static final Pattern QUOTED_INTERVAL = Pattern.compile(
            "INTERVAL\\s+'(-?\\d+)'\\s+(DAY|DAYS|HOUR|HOURS|MONTH|MONTHS|YEAR|YEARS|MINUTE|MINUTES)",
            Pattern.CASE_INSENSITIVE);

    public static String toSpark(String sql) {
        String result = rewriteDateAdd(sql);
        return rewriteInterval(result);
    }

    private static String rewriteDateAdd(String sql) {
        Matcher matcher = DATE_ADD.matcher(sql);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            int days = Integer.parseInt(matcher.group(1));
            String expr = matcher.group(2);
            String replacement = days < 0
                    ? "date_sub(" + expr + ", " + (-days) + ")"
                    : "date_add(" + expr + ", " + days + ")";
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private static String rewriteInterval(String sql) {
        Matcher matcher = QUOTED_INTERVAL.matcher(sql);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String unit = matcher.group(2).toUpperCase();
            if (!unit.endsWith("S")) {
                unit = unit + "S";
            }
            matcher.appendReplacement(sb, Matcher.quoteReplacement("INTERVAL " + matcher.group(1) + " " + unit));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
