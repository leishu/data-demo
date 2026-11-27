package com.demo.lake.common.lineage;

import com.demo.lake.common.sql.SqlScript;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 从转换 SQL 头部的 {@code @lineage} 注释解析血缘声明。
 *
 * <p>声明就写在产生它的 SQL 旁边：改了 SQL 却忘了改血缘时，守卫单测会立刻发现列或表对不上。
 * 注释对 Spark 无害，执行前由 {@link SqlScript#stripComments(String)} 去掉。</p>
 */
@UtilityClass
public final class LineageParser {

    private static final Pattern TABLE = Pattern.compile(
            "^--\\s*@lineage\\s+table:\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*->\\s*([A-Za-z_][A-Za-z0-9_]*)\\s*$");
    private static final Pattern COLUMN = Pattern.compile(
            "^--\\s*@lineage\\s+column:\\s*([A-Za-z_][A-Za-z0-9_]*)\\.([A-Za-z_][A-Za-z0-9_]*)\\s*"
                    + "->\\s*([A-Za-z_][A-Za-z0-9_]*)\\.([A-Za-z_][A-Za-z0-9_]*)\\s*$");

    public static List<TableEdge> tables(String resource) {
        List<TableEdge> out = new ArrayList<>();
        for (String line : SqlScript.readResource(resource).split("\\R")) {
            Matcher matcher = TABLE.matcher(line.trim());
            if (matcher.matches()) {
                out.add(new TableEdge(matcher.group(1), matcher.group(2), transformOf(resource)));
            }
        }
        return List.copyOf(out);
    }

    public static List<ColumnEdge> columns(String resource) {
        List<ColumnEdge> out = new ArrayList<>();
        for (String line : SqlScript.readResource(resource).split("\\R")) {
            Matcher matcher = COLUMN.matcher(line.trim());
            if (matcher.matches()) {
                out.add(new ColumnEdge(matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4)));
            }
        }
        return List.copyOf(out);
    }

    /** sql/transform/bronze_to_silver.sql -> bronze_to_silver */
    public static String transformOf(String resource) {
        String name = resource.substring(resource.lastIndexOf('/') + 1);
        return name.endsWith(".sql") ? name.substring(0, name.length() - 4) : name;
    }
}
