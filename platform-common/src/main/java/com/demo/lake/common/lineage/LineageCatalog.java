package com.demo.lake.common.lineage;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.sql.SqlResources;
import lombok.experimental.UtilityClass;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 血缘目录：表级与列级声明从转换 SQL 解析，保留在 common 里供作业和服务共用。
 *
 * <p>和元数据仓不同，这里只声明「设计上的血缘」；真正某次写入对应哪个快照在 {@code ops.lineage_runs}。
 * 两者结合才能从 gold 反查到「哪次运行、哪个快照、上游经过哪些表和列」。</p>
 */
@UtilityClass
public final class LineageCatalog {

    public static final List<String> TABLES = List.of(
            LakeTables.BRONZE_ORDERS, LakeTables.SILVER_ORDERS, LakeTables.GOLD_DAILY_REVENUE);

    public static final List<TableEdge> TABLE_EDGES = tableEdges();
    public static final List<ColumnEdge> COLUMN_EDGES = columnEdges();

    /** 每个作业写入的业务表；LineageWriter 按这张表决定要记录哪些表的快照 */
    private static final Map<String, List<String>> WRITTEN = Map.of(
            LakeJobs.INIT, TABLES,
            LakeJobs.INGEST, List.of(LakeTables.BRONZE_ORDERS),
            LakeJobs.TRANSFORM, List.of(LakeTables.SILVER_ORDERS, LakeTables.GOLD_DAILY_REVENUE),
            LakeJobs.EVOLVE, List.of(LakeTables.SILVER_ORDERS),
            LakeJobs.MAINTAIN, TABLES);

    private static List<TableEdge> tableEdges() {
        List<TableEdge> out = new ArrayList<>();
        out.addAll(LineageParser.tables(SqlResources.TRANSFORM_MERGE));
        out.addAll(LineageParser.tables(SqlResources.TRANSFORM_GOLD));
        return List.copyOf(out);
    }

    private static List<ColumnEdge> columnEdges() {
        List<ColumnEdge> out = new ArrayList<>();
        out.addAll(LineageParser.columns(SqlResources.TRANSFORM_MERGE));
        out.addAll(LineageParser.columns(SqlResources.TRANSFORM_GOLD));
        return List.copyOf(out);
    }

    public static boolean knownTable(String table) {
        return table != null && TABLES.contains(table);
    }

    /** 沿血缘边向上追溯，gold -> silver -> bronze */
    public static List<String> upstream(String table) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(table);
        while (!queue.isEmpty()) {
            String current = queue.removeFirst();
            for (TableEdge edge : TABLE_EDGES) {
                if (edge.getTarget().equals(current) && seen.add(edge.getSource())) {
                    queue.addLast(edge.getSource());
                }
            }
        }
        return List.copyOf(seen);
    }

    public static List<String> downstream(String table) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(table);
        while (!queue.isEmpty()) {
            String current = queue.removeFirst();
            for (TableEdge edge : TABLE_EDGES) {
                if (edge.getSource().equals(current) && seen.add(edge.getTarget())) {
                    queue.addLast(edge.getTarget());
                }
            }
        }
        return List.copyOf(seen);
    }

    /** 产出目标表的列级血缘，按目标列稳定排序 */
    public static List<ColumnEdge> columnsInto(String table) {
        List<ColumnEdge> out = new ArrayList<>();
        for (ColumnEdge edge : COLUMN_EDGES) {
            if (edge.getTargetTable().equals(table)) {
                out.add(edge);
            }
        }
        return List.copyOf(out);
    }

    public static List<String> writtenTables(String job) {
        return WRITTEN.getOrDefault(job, List.of());
    }
}
