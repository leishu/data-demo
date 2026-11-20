package com.demo.lake.trino;

import io.trino.jdbc.QueryStats;
import io.trino.jdbc.TrinoResultSet;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Trino JDBC 薄封装：连一个 coordinator，跑一条语句，拿回列/行/耗时。
 * 不缓存连接池——即席查询 CLI 一次一个会话，够了。
 */
public final class TrinoClient implements AutoCloseable {

    private final Connection connection;
    private final int timeoutSeconds;

    private TrinoClient(Connection connection, int timeoutSeconds) {
        this.connection = connection;
        this.timeoutSeconds = timeoutSeconds;
    }

    public static TrinoClient connect(String url, String user, String source, int timeoutSeconds)
            throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        // 会话时区固定 UTC：湖表里的 timestamp 是 UTC 墙钟，用机器时区解释会整体偏移
        props.setProperty("timezone", "UTC");
        // source 是驱动认的连接属性，会作为查询来源上报到 coordinator
        if (source != null && !source.isBlank()) {
            props.setProperty("source", source);
        }
        Connection conn = DriverManager.getConnection(url, props);
        conn.setReadOnly(true);
        return new TrinoClient(conn, timeoutSeconds);
    }

    public QueryResult execute(String sql, int maxRows) throws SQLException {
        long start = System.nanoTime();
        try (Statement stmt = connection.createStatement()) {
            if (timeoutSeconds > 0) {
                try {
                    stmt.setQueryTimeout(timeoutSeconds);
                } catch (SQLException ignore) {
                    // 驱动不支持就算了，服务端仍有 query.max-run-time 兜底
                }
            }
            try (ResultSet rs = stmt.executeQuery(sql)) {
                ResultSetMetaData md = rs.getMetaData();
                int columnCount = md.getColumnCount();
                List<String> headers = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    headers.add(md.getColumnLabel(i));
                }

                List<List<String>> rows = new ArrayList<>();
                boolean truncated = false;
                while (rs.next()) {
                    if (rows.size() >= maxRows) {
                        truncated = true;
                        break;
                    }
                    List<String> row = new ArrayList<>(columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        row.add(format(rs.getObject(i)));
                    }
                    rows.add(row);
                }

                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                String stats = null;
                // QueryStats 没有重写 toString，直接打会得到类名@hash；这里只挑稳定且对排查有用的字段
                if (rs instanceof TrinoResultSet) {
                    QueryStats s = ((TrinoResultSet) rs).getStats();
                    if (s != null) {
                        stats = String.format(
                                "queryId=%s state=%s elapsed=%dms cpu=%dms splits=%d/%d rows=%d bytes=%d peakMem=%dB",
                                s.getQueryId(), s.getState(), s.getElapsedTimeMillis(), s.getCpuTimeMillis(),
                                s.getCompletedSplits(), s.getTotalSplits(), s.getProcessedRows(),
                                s.getProcessedBytes(), s.getPeakMemoryBytes());
                    }
                }
                return new QueryResult(headers, rows, truncated, elapsedMs, stats);
            }
        }
    }

    private static String format(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        if (value instanceof Timestamp) {
            // Timestamp.toString() 按 JVM 默认时区渲染（本机 UTC+8，会把 UTC 时刻显示成 +8），
            // 这里输出 ISO-8601 绝对时刻，display 和 session 时区都无关
            return ((Timestamp) value).toInstant().toString();
        }
        if (value instanceof java.sql.Date || value instanceof java.sql.Time) {
            return value.toString();
        }
        if (value instanceof byte[]) {
            return "<binary " + ((byte[]) value).length + "B>";
        }
        return String.valueOf(value);
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException ignore) {
            // close 失败无需处理
        }
    }

    /** 一次查询的结果快照 */
    @Getter
    @Accessors(fluent = true)
    @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
    public static final class QueryResult {
        private final List<String> headers;
        private final List<List<String>> rows;
        private final boolean truncated;
        private final long elapsedMs;
        private final String stats;
    }
}
