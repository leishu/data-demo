package com.demo.lake.adhoc;

import io.trino.jdbc.QueryStats;
import io.trino.jdbc.TrinoResultSet;

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
 * 官方 trino-jdbc 的薄封装：连一个 coordinator，跑一条语句，拿回列/行/耗时/stats。
 * 不缓存连接池——即席查询一次一个会话，够了；协议层完全由官方驱动负责，这里只是调用与结果整形。
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
        Properties properties = new Properties();
        properties.setProperty("user", user);
        // 会话时区固定 UTC：湖表里的 timestamp 是 UTC 墙钟，用机器时区解释会整体偏移
        properties.setProperty("timezone", "UTC");
        if (source != null && !source.isBlank()) {
            properties.setProperty("source", source);
        }
        Connection conn = DriverManager.getConnection(url, properties);
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
                if (rs instanceof TrinoResultSet) {
                    QueryStats queryStats = ((TrinoResultSet) rs).getStats();
                    if (queryStats != null) {
                        stats = String.format(
                                "queryId=%s state=%s elapsed=%dms cpu=%dms splits=%d/%d rows=%d bytes=%d peakMem=%dB",
                                queryStats.getQueryId(), queryStats.getState(), queryStats.getElapsedTimeMillis(),
                                queryStats.getCpuTimeMillis(), queryStats.getCompletedSplits(),
                                queryStats.getTotalSplits(), queryStats.getProcessedRows(),
                                queryStats.getProcessedBytes(), queryStats.getPeakMemoryBytes());
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
            // Timestamp.toString() 按 JVM 默认时区渲染，这里输出 ISO-8601 绝对时刻
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
    public static final class QueryResult {
        private final List<String> headers;
        private final List<List<String>> rows;
        private final boolean truncated;
        private final long elapsedMs;
        private final String stats;

        QueryResult(List<String> headers, List<List<String>> rows, boolean truncated,
                    long elapsedMs, String stats) {
            this.headers = headers;
            this.rows = rows;
            this.truncated = truncated;
            this.elapsedMs = elapsedMs;
            this.stats = stats;
        }

        public List<String> headers() { return headers; }
        public List<List<String>> rows() { return rows; }
        public boolean truncated() { return truncated; }
        public long elapsedMs() { return elapsedMs; }
        public String stats() { return stats; }
    }
}
