package com.demo.lake.adhoc;

import java.util.ArrayList;
import java.util.List;

/** 把结果集渲染成带边框的文本表格（纯显示层，处理 CJK 宽字符对齐） */
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
        String text = value.replace("\n", " ");
        if (text.codePointCount(0, text.length()) > MAX_CELL) {
            text = text.substring(0, text.offsetByCodePoints(0, MAX_CELL - 1)) + "…";
        }
        return text;
    }

    private static int displayWidth(String value) {
        String text = cell(value);
        int width = 0;
        for (int i = 0; i < text.length(); ) {
            int codePoint = text.codePointAt(i);
            width += codePointWidth(codePoint);
            i += Character.charCount(codePoint);
        }
        return width;
    }

    /** 组合符号不占列；CJK / 全角 / emoji 在等宽终端里占两列，否则含中文的表头会错位 */
    private static int codePointWidth(int codePoint) {
        switch (Character.getType(codePoint)) {
            case Character.NON_SPACING_MARK:
            case Character.ENCLOSING_MARK:
            case Character.COMBINING_SPACING_MARK:
                return 0;
            default:
                break;
        }
        if (codePoint == 0x200B || codePoint == 0x200D || codePoint == 0xFE0F) {
            return 0;
        }
        return isWide(codePoint) ? 2 : 1;
    }

    private static boolean isWide(int codePoint) {
        return (codePoint >= 0x1100 && codePoint <= 0x115F)            // 韩文字母
                || codePoint == 0x2329 || codePoint == 0x232A
                || (codePoint >= 0x2E80 && codePoint <= 0xA4CF && codePoint != 0x303F) // CJK 部首、假名、汉字等
                || codePoint >= 0xAC00 && codePoint <= 0xD7A3            // 韩文音节
                || codePoint >= 0xF900 && codePoint <= 0xFAFF            // CJK 兼容汉字
                || codePoint >= 0xFE10 && codePoint <= 0xFE19            // 竖排标点
                || codePoint >= 0xFE30 && codePoint <= 0xFE6F            // CJK 兼容形式
                || codePoint >= 0xFF00 && codePoint <= 0xFF60            // 全角字符
                || codePoint >= 0xFFE0 && codePoint <= 0xFFE6
                || codePoint >= 0x1F300 && codePoint <= 0x1FAFF          // emoji
                || codePoint >= 0x20000 && codePoint <= 0x3FFFD;         // CJK 扩展区
    }

    private static void appendLine(StringBuilder sb, int[] widths) {
        sb.append('+');
        for (int width : widths) {
            sb.append("-".repeat(width + 2)).append('+');
        }
        sb.append('\n');
    }

    private static void appendRow(StringBuilder sb, List<String> values, int[] widths) {
        sb.append('|');
        for (int i = 0; i < widths.length; i++) {
            String text = cell(values.get(i));
            int pad = widths[i] - displayWidth(text);
            sb.append(' ').append(text).append(" ".repeat(pad)).append(" |");
        }
        sb.append('\n');
    }

    public static List<String> row(List<String> values) {
        return new ArrayList<>(values);
    }
}
