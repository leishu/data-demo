package com.demo.lake.common.sql;

import lombok.experimental.UtilityClass;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@code ${var}} 占位符渲染器。
 *
 * <p>DDL / 转换 SQL / 迁移脚本外置成 classpath 下的 .sql 资源后，由这里把配置值绑进模板。
 * 语法与 {@code SqlParams} 的即席查询参数化保持一致：未提供的占位符保留原样，
 * 方便调用方自行判定「缺参」（比如即席查询整条跳过）。</p>
 */
@UtilityClass
public final class SqlTemplate {

    private static final Pattern VAR = Pattern.compile("\\$\\{([a-zA-Z0-9_.-]+)}");

    /** 渲染模板；vars 里没有的 key 保留原占位符。 */
    public static String render(String template, Map<String, String> vars) {
        if (vars.isEmpty()) {
            return template;
        }
        Matcher matcher = VAR.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String value = vars.get(matcher.group(1));
            matcher.appendReplacement(sb, Matcher.quoteReplacement(value == null ? matcher.group(0) : value));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /** 读 classpath 资源并渲染，资源不存在时报 IllegalArgumentException（与 SqlScript 一致）。 */
    public static String renderResource(String resource, Map<String, String> vars) {
        return render(SqlScript.readResource(resource), vars);
    }

    /** 渲染后仍残留 {@code ${...}} 即为缺参，可用于模板的单测守卫。 */
    public static boolean hasUnresolved(String sql) {
        return VAR.matcher(sql).find();
    }
}
