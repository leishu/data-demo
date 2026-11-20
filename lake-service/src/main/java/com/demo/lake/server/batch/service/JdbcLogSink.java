package com.demo.lake.server.batch.service;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 把每个 run 的日志尾部搬进 Postgres：本地进程虽然跑在某台实例上，
 * 但 tail / SSE 从任意实例都读得到。每个 run 只留最近 {@value #MAX_LINES} 行，够排障即可。
 */
@Component
@Slf4j
public class JdbcLogSink implements LogSink {

    private static final String TABLE = "lake_batch.batch_log_lines";
    private static final int MAX_LINES = 2000;

    @Qualifier("stateDataSource")
    private final DataSource dataSource;

    public JdbcLogSink(@Qualifier("stateDataSource") DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @PostConstruct
    public void init() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS lake_batch");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (\n"
                    + "  run_id VARCHAR(80) NOT NULL,\n"
                    + "  seq BIGINT NOT NULL,\n"
                    + "  line VARCHAR(2000),\n"
                    + "  at_ms BIGINT NOT NULL,\n"
                    + "  PRIMARY KEY (run_id, seq)\n"
                    + ")");
            log.info("作业日志表就绪: {}", TABLE);
        } catch (SQLException e) {
            log.warn("作业日志表初始化失败（跨实例 tail 暂不可用）: {}", e.getMessage());
        }
    }

    @Override
    public void append(String runId, List<String> lines) {
        if (lines == null || lines.isEmpty()) {
            return;
        }
        try (Connection conn = dataSource.getConnection()) {
            long seq = maxSeq(conn, runId);
            long now = System.currentTimeMillis();
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + TABLE + " (run_id, seq, line, at_ms) VALUES (?,?,?,?)")) {
                for (String line : lines) {
                    ps.setString(1, runId);
                    ps.setLong(2, ++seq);
                    ps.setString(3, truncate(line));
                    ps.setLong(4, now);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            // 只留尾部，避免一次长日志把状态库撑爆
            long cutoff = seq - MAX_LINES;
            if (cutoff > 0) {
                try (PreparedStatement ps = conn.prepareStatement(
                        "DELETE FROM " + TABLE + " WHERE run_id=? AND seq<=?")) {
                    ps.setString(1, runId);
                    ps.setLong(2, cutoff);
                    ps.executeUpdate();
                }
            }
        } catch (SQLException e) {
            // 日志是旁路：写失败不能影响作业本身或终态落库
            log.warn("作业日志写入失败 run={}: {}", runId, e.getMessage());
        }
    }

    @Override
    public List<LogLine> tail(String runId, int lines) {
        String sql = "SELECT seq, line FROM (SELECT seq, line FROM " + TABLE
                + " WHERE run_id=? ORDER BY seq DESC LIMIT ?) t ORDER BY seq ASC";
        return query(runId, ps -> {
            ps.setString(1, runId);
            ps.setInt(2, Math.max(1, lines));
        }, sql);
    }

    @Override
    public List<LogLine> since(String runId, long afterSeq, int limit) {
        return query(runId, ps -> {
            ps.setString(1, runId);
            ps.setLong(2, afterSeq);
            ps.setInt(3, Math.max(1, limit));
        }, "SELECT seq, line FROM " + TABLE + " WHERE run_id=? AND seq>? ORDER BY seq LIMIT ?");
    }

    @Override
    public int purge(Instant before) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("DELETE FROM " + TABLE + " WHERE at_ms<?")) {
            ps.setLong(1, before.toEpochMilli());
            return ps.executeUpdate();
        } catch (SQLException e) {
            log.warn("过期作业日志清理失败: {}", e.getMessage());
            return 0;
        }
    }

    private static long maxSeq(Connection conn, String runId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT coalesce(max(seq), 0) FROM " + TABLE + " WHERE run_id=?")) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private List<LogLine> query(String runId, Binder binder, String sql) {
        List<LogLine> out = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.bind(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new LogLine(rs.getLong("seq"), rs.getString("line")));
                }
            }
        } catch (SQLException e) {
            log.debug("读作业日志失败 run={}: {}", runId, e.getMessage());
        }
        return out;
    }

    private static String truncate(String line) {
        if (line == null) {
            return null;
        }
        return line.length() <= 2000 ? line : line.substring(0, 1997) + "...";
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement ps) throws SQLException;
    }
}
