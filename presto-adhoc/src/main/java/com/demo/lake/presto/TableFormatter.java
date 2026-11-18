package com.demo.lake.presto;

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
        return cell(value).codePointCount(0, cell(value).length());
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
            int pad = widths[i] - v.codePointCount(0, v.length());
            sb.append(' ').append(v).append(" ".repeat(pad)).append(" |");
        }
        sb.append('\n');
    }

    public static List<String> row(List<String> values) {
        return new ArrayList<>(values);
    }
}
