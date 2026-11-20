package com.demo.lake.trino;

import java.util.ArrayList;
import java.util.List;

/** 把结果集渲染成带边框的文本表格（终端里看 ad-hoc 结果够用了，不导出 CSV） */
public final class TableFormatter {

    private static final int MAX_CELL = 60;

    private TableFormatter() {
    }

    public static String render(List<String> headers, List<List<String>> rows) {
        int cols = headers.size();
        int[] widths = new int[cols];
        for (int i = 0; i < cols; i++) {
            widths[i] = displayWidth(headers.get(i));
        }
        for (List<String> row : rows) {
            for (int i = 0; i < cols; i++) {
                widths[i] = Math.max(widths[i], displayWidth(row.get(i)));
            }
        }

        StringBuilder sb = new StringBuilder();
        appendLine(sb, widths);
        appendRow(sb, headers, widths);
        appendLine(sb, widths);
        for (List<String> row : rows) {
            appendRow(sb, row, widths);
        }
        appendLine(sb, widths);
        return sb.toString();
    }

    private static String cell(String value) {
        if (value == null) {
            return "NULL";
        }
        String v = value.replace("\n", " ");
        if (v.codePointCount(0, v.length()) > MAX_CELL) {
            v = v.substring(0, v.offsetByCodePoints(0, MAX_CELL - 1)) + "…";
        }
        return v;
    }

    private static int displayWidth(String value) {
        String v = cell(value);
        int width = 0;
        for (int i = 0; i < v.length(); ) {
            int cp = v.codePointAt(i);
            width += codePointWidth(cp);
            i += Character.charCount(cp);
        }
        return width;
    }

    /** 组合符号不占列；CJK / 全角 / emoji 在等宽终端里占两列，否则含中文的表头会错位 */
    private static int codePointWidth(int cp) {
        switch (Character.getType(cp)) {
            case Character.NON_SPACING_MARK:
            case Character.ENCLOSING_MARK:
            case Character.COMBINING_SPACING_MARK:
                return 0;
            default:
                break;
        }
        if (cp == 0x200B || cp == 0x200D || cp == 0xFE0F) {
            return 0;
        }
        return isWide(cp) ? 2 : 1;
    }

    private static boolean isWide(int cp) {
        return (cp >= 0x1100 && cp <= 0x115F)            // 韩文字母
                || cp == 0x2329 || cp == 0x232A
                || (cp >= 0x2E80 && cp <= 0xA4CF && cp != 0x303F) // CJK 部首、假名、汉字等
                || (cp >= 0xAC00 && cp <= 0xD7A3)        // 韩文音节
                || (cp >= 0xF900 && cp <= 0xFAFF)        // CJK 兼容汉字
                || (cp >= 0xFE10 && cp <= 0xFE19)        // 竖排标点
                || (cp >= 0xFE30 && cp <= 0xFE6F)        // CJK 兼容形式
                || (cp >= 0xFF00 && cp <= 0xFF60)        // 全角字符
                || (cp >= 0xFFE0 && cp <= 0xFFE6)
                || (cp >= 0x1F300 && cp <= 0x1FAFF)      // emoji
                || (cp >= 0x20000 && cp <= 0x3FFFD);     // CJK 扩展区
    }

    private static void appendLine(StringBuilder sb, int[] widths) {
        sb.append('+');
        for (int w : widths) {
            sb.append("-".repeat(w + 2)).append('+');
        }
        sb.append('\n');
    }

    private static void appendRow(StringBuilder sb, List<String> values, int[] widths) {
        sb.append('|');
        for (int i = 0; i < widths.length; i++) {
            String v = cell(values.get(i));
            int pad = widths[i] - displayWidth(v);
            sb.append(' ').append(v).append(" ".repeat(pad)).append(" |");
        }
        sb.append('\n');
    }

    public static List<String> row(List<String> values) {
        return new ArrayList<>(values);
    }
}
