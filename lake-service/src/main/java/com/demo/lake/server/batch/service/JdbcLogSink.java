package com.demo.lake.server.batch.service;

import com.demo.lake.server.support.DbLocks;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 把每个 run 的日志尾部搬进 Postgres：本地进程虽然跑在某台实例上，
 * 但 tail / SSE 从任意实例都读得到。每个 run 只留最近 {@value #MAX_LINES} 行，够排障即可。
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class JdbcLogSink implements LogSink {

    private static final String TABLE = "lake_batch.batch_log_lines";
    private static final int MAX_LINES = 2000;

    @Qualifier("stateJdbc")
    private final JdbcTemplate jdbc;
    @Qualifier("stateTx")
    private final TransactionTemplate tx;

    @PostConstruct
    public void init() {
        try {
            jdbc.execute("CREATE SCHEMA IF NOT EXISTS lake_batch");
            jdbc.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (\n"
                    + "  run_id VARCHAR(80) NOT NULL,\n"
                    + "  seq BIGINT NOT NULL,\n"
                    + "  line VARCHAR(2000),\n"
                    + "  at_ms BIGINT NOT NULL,\n"
                    + "  PRIMARY KEY (run_id, seq)\n"
                    + ")");
            DbLocks.ensure(jdbc.getDataSource(), DbLocks.LOG_APPEND);
            log.info("作业日志表就绪: {}", TABLE);
        } catch (DataAccessException e) {
            log.warn("作业日志表初始化失败（跨实例 tail 暂不可用）: {}", e.getMessage());
        }
    }

    @Override
    public void append(String runId, List<String> lines) {
        if (lines == null || lines.isEmpty()) {
            return;
        }
        try {
            tx.execute(status -> jdbc.execute((ConnectionCallback<Void>) conn -> {
                // MAX(seq)+1 必须在锁内：同一 run 出现第二个写入者时，否则会主键冲突整批丢日志
                DbLocks.lock(conn, DbLocks.LOG_APPEND);
                long seq = jdbc.queryForObject(
                        "SELECT COALESCE(MAX(seq), 0) FROM " + TABLE + " WHERE run_id=?", Long.class, runId);
                long now = System.currentTimeMillis();
                List<Object[]> batch = new ArrayList<>(lines.size());
                for (String line : lines) {
                    batch.add(new Object[]{runId, ++seq, truncate(line), now});
                }
                jdbc.batchUpdate("INSERT INTO " + TABLE + " (run_id, seq, line, at_ms) VALUES (?,?,?,?)", batch);
                // 只留尾部，避免一次长日志把状态库撑爆
                long cutoff = seq - MAX_LINES;
                if (cutoff > 0) {
                    jdbc.update("DELETE FROM " + TABLE + " WHERE run_id=? AND seq<=?",
                            new Object[]{runId, cutoff}, new int[]{Types.VARCHAR, Types.BIGINT});
                }
                return null;
            }));
        } catch (DataAccessException e) {
            // 日志是旁路：写失败不能影响作业本身或终态落库
            log.warn("作业日志写入失败 run={}: {}", runId, e.getMessage());
        }
    }

    @Override
    public List<LogLine> tail(String runId, int lines) {
        return jdbc.query("SELECT seq, line FROM (SELECT seq, line FROM " + TABLE
                        + " WHERE run_id=? ORDER BY seq DESC LIMIT ?) t ORDER BY seq ASC",
                new Object[]{runId, Math.max(1, lines)},
                (rs, n) -> new LogLine(rs.getLong("seq"), rs.getString("line")));
    }

    @Override
    public List<LogLine> since(String runId, long afterSeq, int limit) {
        return jdbc.query("SELECT seq, line FROM " + TABLE + " WHERE run_id=? AND seq>? ORDER BY seq LIMIT ?",
                new Object[]{runId, afterSeq, Math.max(1, limit)},
                (rs, n) -> new LogLine(rs.getLong("seq"), rs.getString("line")));
    }

    @Override
    public int purge(Instant before) {
        try {
            return jdbc.update("DELETE FROM " + TABLE + " WHERE at_ms<?", before.toEpochMilli());
        } catch (DataAccessException e) {
            log.warn("过期作业日志清理失败: {}", e.getMessage());
            return 0;
        }
    }

    private static String truncate(String line) {
        if (line == null) {
            return null;
        }
        return line.length() <= 2000 ? line : line.substring(0, 1997) + "...";
    }
}
