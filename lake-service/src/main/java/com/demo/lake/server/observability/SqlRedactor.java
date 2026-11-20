package com.demo.lake.server.observability;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * 保险箱写入前的尽力脱敏：保留 SQL 结构（表、列、函数、比较符），只处理字符串字面量。
 *
 * <p>这是启发式规则而非完整 SQL 解析：日期、短数字、短字母数字 ID 保留（合规回溯通常要看
 * 「哪天、哪个商户」），其余字符串掩码；出现 password/token/phone/id_card 等上下文时一律掩码。
 * 合规要求逐字原文时把 {@code lake.audit.redact} 关掉，并按最高敏感级管理保险箱。</p>
 */
public final class SqlRedactor {

    private static final Pattern DATE = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final Pattern SAFE_NUMBER = Pattern.compile("-?\\d{1,10}(\\.\\d{1,6})?");
    /** 像 M1 / O-123 / gold_daily_revenue 这类短标识，掩码了反而没法审计 */
    private static final Pattern SAFE_TOKEN = Pattern.compile("[A-Za-z][A-Za-z0-9_-]{0,30}");
    private static final Pattern SENSITIVE = Pattern.compile(
            "(?i)(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|id[_-]?card|phone|mobile|credit[_-]?card)");

    private SqlRedactor() {
    }

    public static String redact(String sql) {
        if (sql == null) {
            return null;
        }
        String normalized = sql.replaceAll("(?s)/\\*.*?\\*/", " ")
                .replaceAll("(?m)--[^\\n]*", " ")
                .replaceAll("\\s+", " ")
                .trim();

        StringBuilder out = new StringBuilder(normalized.length());
        int i = 0;
        while (i < normalized.length()) {
            char c = normalized.charAt(i);
            if (c != '\'') {
                out.append(c);
                i++;
                continue;
            }
            int end = literalEnd(normalized, i);
            String literal = normalized.substring(i + 1, end - 1).replace("''", "'");
            // 只看当前谓词的上下文，避免前一个 password='...' 把后面的 dt='2026-11-20' 也误掩码
            String before = normalized.substring(Math.max(0, i - 64), i).toLowerCase(Locale.ROOT);
            int cut = Math.max(Math.max(before.lastIndexOf(" and "), before.lastIndexOf(" or ")),
                    Math.max(before.lastIndexOf(" where "), before.lastIndexOf(", ")));
            // 别名也可能暴露语义：'x' as password / as pwd 要看字面量后面的片段
            String after = normalized.substring(end, Math.min(normalized.length(), end + 24)).toLowerCase(Locale.ROOT);
            String context = (cut >= 0 ? before.substring(cut) : before) + after;
            out.append(mask(literal, context));
            i = end;
        }
        return out.toString();
    }

    /** 返回单引号字面量结束后的下标；SQL 里用 '' 表示一个单引号 */
    private static int literalEnd(String sql, int start) {
        int i = start + 1;
        while (i < sql.length()) {
            if (sql.charAt(i) == '\'') {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i++;
        }
        return sql.length();
    }

    private static String mask(String literal, String context) {
        if (!SENSITIVE.matcher(context).find()
                && (DATE.matcher(literal).matches()
                || SAFE_NUMBER.matcher(literal).matches()
                || SAFE_TOKEN.matcher(literal).matches())) {
            return "'" + literal + "'";
        }
        return "'***'";
    }
}
