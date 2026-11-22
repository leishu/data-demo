package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.support.DbLocks;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
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
import java.util.Optional;

/**
 * 用 Postgres 存运行态（复用 infra 的元数据库，不引入新组件）。
 *
 * <p>表在服务启动时幂等创建；SQL 全是固定语句 + 占位符，没有 SQL 注入面。
 * 每条记录带 owner / heartbeat_ms：心跳只由「执行器还活着」的 run 续期，失联的对账后落终态。</p>
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class JdbcRunStateStore implements RunStateStore {

    private static final String TABLE = "lake_batch.batch_runs";

    @Qualifier("stateDataSource")
    private final DataSource dataSource;

    @PostConstruct
    public void init() {
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS lake_batch");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (\n"
                    + "  run_id VARCHAR(80) PRIMARY KEY,\n"
                    + "  job VARCHAR(64) NOT NULL,\n"
                    + "  command VARCHAR(1000),\n"
                    + "  status VARCHAR(16) NOT NULL,\n"
                    + "  exit_code INTEGER,\n"
                    + "  started_ms BIGINT NOT NULL,\n"
                    + "  finished_ms BIGINT,\n"
                    + "  duration_ms BIGINT,\n"
                    + "  log_file VARCHAR(1000),\n"
                    + "  message VARCHAR(1000),\n"
                    + "  owner VARCHAR(80) NOT NULL,\n"
                    + "  heartbeat_ms BIGINT NOT NULL,\n"
                    + "  updated_ms BIGINT NOT NULL,\n"
                    + "  handle VARCHAR(255)\n"
                    + ")");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_lake_batch_runs_started ON " + TABLE + "(started_ms DESC)");
            // 老库升级：幂等/调度引入的新列按需补上（Postgres 与 H2 都支持 information_schema）
            ensureColumn(conn, "handle", "handle VARCHAR(255)");
            stmt.execute("CREATE TABLE IF NOT EXISTS lake_batch.idempotency (\n"
                    + "  idem_key VARCHAR(128) PRIMARY KEY,\n"
                    + "  run_id VARCHAR(80) NOT NULL\n"
                    + ")");
            DbLocks.ensure(dataSource, DbLocks.SUBMIT, DbLocks.DISPATCH);
            log.info("运行状态表就绪: {}", TABLE);
        } catch (SQLException e) {
            // 不阻止启动：infra 没起时只读查询还能用，批作业接口会返回 503
            log.warn("运行状态表初始化失败（批作业接口暂不可用）: {}", e.getMessage());
        }
    }

    private static void ensureColumn(Connection conn, String name, String ddl) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT 1 FROM information_schema.columns WHERE LOWER(table_schema)='lake_batch'"
                        + " AND LOWER(table_name)='batch_runs' AND LOWER(column_name)=?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return;
                }
            }
        }
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER TABLE " + TABLE + " ADD COLUMN " + ddl);
        }
    }

    private static final String SELECT = "SELECT run_id, job, command, status, exit_code, started_ms,"
            + " finished_ms, duration_ms, log_file, message FROM " + TABLE;

    private static final String INSERT_RUN = "INSERT INTO " + TABLE + " (run_id, job, command, status, exit_code,"
            + " started_ms, finished_ms, duration_ms, log_file, message, owner, heartbeat_ms, updated_ms)"
            + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

    @Override
    public void insert(BatchRun run, String owner) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(INSERT_RUN)) {
            bindRun(ps, run, owner);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("运行状态库写入失败: " + e.getMessage(), e);
        }
    }

    @Override
    public String insertWithIdempotency(BatchRun run, String owner, String key) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // 全局提交锁：同一时刻只有一个请求在做「查键 + 插记录 + 登记键」
                DbLocks.lock(conn, DbLocks.SUBMIT);
                String existing = findIdempotencyRunId(conn, key);
                if (existing != null) {
                    conn.commit();
                    return existing;
                }
                try (PreparedStatement ps = conn.prepareStatement(INSERT_RUN)) {
                    bindRun(ps, run, owner);
                    ps.executeUpdate();
                }
                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO lake_batch.idempotency (idem_key, run_id) VALUES (?,?)")) {
                    ps.setString(1, key);
                    ps.setString(2, run.id());
                    ps.executeUpdate();
                }
                conn.commit();
                return run.id();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("运行状态库写入失败: " + e.getMessage(), e);
        }
    }

    private static void bindRun(PreparedStatement ps, BatchRun run, String owner) throws SQLException {
        ps.setString(1, run.id());
        ps.setString(2, run.job());
        ps.setString(3, run.command());
        ps.setString(4, run.status());
        setInt(ps, 5, run.exitCode());
        ps.setLong(6, run.startedAt().toEpochMilli());
        setNullableLong(ps, 7, run.finishedAt() == null ? null : run.finishedAt().toEpochMilli());
        setNullableLong(ps, 8, run.durationMs());
        ps.setString(9, run.logFile());
        ps.setString(10, run.message());
        ps.setString(11, owner);
        ps.setLong(12, System.currentTimeMillis());
        ps.setLong(13, System.currentTimeMillis());
    }

    @Override
    public void finish(BatchRun run) {
        execute("UPDATE " + TABLE + " SET status=?, exit_code=?, finished_ms=?, duration_ms=?,"
                        + " message=?, updated_ms=? WHERE run_id=?", ps -> {
            ps.setString(1, run.status());
            setInt(ps, 2, run.exitCode());
            ps.setLong(3, run.finishedAt() == null ? System.currentTimeMillis() : run.finishedAt().toEpochMilli());
            setNullableLong(ps, 4, run.durationMs());
            ps.setString(5, run.message());
            ps.setLong(6, System.currentTimeMillis());
            ps.setString(7, run.id());
        });
    }

    @Override
    public Optional<BatchRun> find(String id) {
        return queryOne(SELECT + " WHERE run_id=?", ps -> ps.setString(1, id));
    }

    @Override
    public List<BatchRun> recent(int limit) {
        return queryList(SELECT + " ORDER BY started_ms DESC LIMIT ?", ps -> ps.setInt(1, limit));
    }

    @Override
    public void heartbeat(String runId, Instant now) {
        execute("UPDATE " + TABLE + " SET heartbeat_ms=?, updated_ms=? WHERE run_id=? AND status='RUNNING'", ps -> {
            ps.setLong(1, now.toEpochMilli());
            ps.setLong(2, now.toEpochMilli());
            ps.setString(3, runId);
        });
    }

    @Override
    public List<BatchRun> staleRunning(Instant before) {
        return queryList(SELECT + " WHERE status='RUNNING' AND heartbeat_ms < ? ORDER BY started_ms",
                ps -> ps.setLong(1, before.toEpochMilli()));
    }

    @Override
    public void markLost(String id, String message, Instant now) {
        execute("UPDATE " + TABLE + " SET status='FAILED', exit_code=1, finished_ms=?, duration_ms=? - started_ms,"
                        + " message=?, updated_ms=? WHERE run_id=? AND status='RUNNING'", ps -> {
            ps.setLong(1, now.toEpochMilli());
            ps.setLong(2, now.toEpochMilli());
            ps.setString(3, message);
            ps.setLong(4, now.toEpochMilli());
            ps.setString(5, id);
        });
    }

    @Override
    public Optional<BatchRun> findByIdempotencyKey(String key) {
        String runId;
        try (Connection conn = dataSource.getConnection()) {
            runId = findIdempotencyRunId(conn, key);
        } catch (SQLException e) {
            throw new IllegalStateException("读幂等键失败: " + e.getMessage(), e);
        }
        return runId == null ? Optional.empty() : find(runId);
    }

    private static String findIdempotencyRunId(Connection conn, String key) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT run_id FROM lake_batch.idempotency WHERE idem_key=?")) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    @Override
    public void recordIdempotency(String key, String runId) {
        if (findByIdempotencyKey(key).isPresent()) {
            return;
        }
        try {
            execute("INSERT INTO lake_batch.idempotency (idem_key, run_id) VALUES (?,?)", ps -> {
                ps.setString(1, key);
                ps.setString(2, runId);
            });
        } catch (RuntimeException e) {
            // 并发重复提交：别人先插了，按已存在的记录处理
            log.debug("幂等键已存在 {}: {}", key, e.getMessage());
        }
    }

    @Override
    public int runningCount() {
        return countByStatus("RUNNING");
    }

    @Override
    public int queuedCount() {
        return countByStatus("QUEUED");
    }

    private int countByStatus(String status) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM " + TABLE + " WHERE status=?")) {
            ps.setString(1, status);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("读运行状态失败: " + e.getMessage(), e);
        }
    }

    @Override
    public BatchRun claimNext(String owner) {
        try (Connection conn = dataSource.getConnection()) {
            return claimNextOnConnection(conn, owner);
        } catch (SQLException e) {
            throw new IllegalStateException("运行状态库读取失败: " + e.getMessage(), e);
        }
    }

    @Override
    public BatchRun claimNextIfSlotAvailable(String owner, int maxConcurrent) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // 名额检查 + 认领在同一把全局行锁内完成，跨实例也不会一起超员
                DbLocks.lock(conn, DbLocks.DISPATCH);
                if (countRunning(conn) >= maxConcurrent) {
                    conn.rollback();
                    return null;
                }
                BatchRun claimed = claimNextOnConnection(conn, owner);
                conn.commit();
                return claimed;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("运行状态库读取失败: " + e.getMessage(), e);
        }
    }

    private static BatchRun claimNextOnConnection(Connection conn, String owner) throws SQLException {
        String id = null;
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT run_id FROM " + TABLE + " WHERE status='QUEUED' ORDER BY started_ms LIMIT 1");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                id = rs.getString(1);
            }
        }
        if (id == null) {
            return null;
        }
        long now = System.currentTimeMillis();
        int changed;
        try (PreparedStatement ps = conn.prepareStatement("UPDATE " + TABLE
                + " SET status='RUNNING', owner=?, heartbeat_ms=?, updated_ms=?"
                + " WHERE run_id=? AND status='QUEUED'")) {
            ps.setString(1, owner);
            ps.setLong(2, now);
            ps.setLong(3, now);
            ps.setString(4, id);
            changed = ps.executeUpdate();
        }
        // 多实例可能同时看到同一个候选；只有抢到 update 的那个实例真正执行
        return changed == 0 ? null : findOnConnection(conn, id).orElse(null);
    }

    private static Optional<BatchRun> findOnConnection(Connection conn, String id) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(SELECT + " WHERE run_id=?")) {
            ps.setString(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(map(rs)) : Optional.empty();
            }
        }
    }

    private static int countRunning(Connection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + TABLE + " WHERE status='RUNNING'");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Override
    public void setHandle(String runId, String handle) {
        execute("UPDATE " + TABLE + " SET handle=?, updated_ms=? WHERE run_id=?", ps -> {
            ps.setString(1, handle);
            ps.setLong(2, System.currentTimeMillis());
            ps.setString(3, runId);
        });
    }

    @FunctionalInterface
    private interface StatementBinder {
        void bind(PreparedStatement ps) throws SQLException;
    }

    private void execute(String sql, StatementBinder binder) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.bind(ps);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("运行状态库写入失败: " + e.getMessage(), e);
        }
    }

    private int executeCount(String sql, StatementBinder binder) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.bind(ps);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("运行状态库写入失败: " + e.getMessage(), e);
        }
    }

    private Optional<BatchRun> queryOne(String sql, StatementBinder binder) {
        List<BatchRun> rows = queryList(sql, binder);
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    private List<BatchRun> queryList(String sql, StatementBinder binder) {
        List<BatchRun> out = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            binder.bind(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(map(rs));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("运行状态库读取失败: " + e.getMessage(), e);
        }
        return out;
    }

    private static BatchRun map(ResultSet rs) throws SQLException {
        Long finished = (Long) rs.getObject("finished_ms");
        Long duration = (Long) rs.getObject("duration_ms");
        Integer exit = (Integer) rs.getObject("exit_code");
        return new BatchRun(
                rs.getString("run_id"),
                rs.getString("job"),
                rs.getString("command"),
                rs.getString("status"),
                exit,
                Instant.ofEpochMilli(rs.getLong("started_ms")),
                finished == null ? null : Instant.ofEpochMilli(finished),
                duration,
                rs.getString("log_file"),
                rs.getString("message"));
    }

    private static void setInt(PreparedStatement ps, int index, Integer value) throws SQLException {
        if (value == null) {
            ps.setNull(index, java.sql.Types.INTEGER);
        } else {
            ps.setInt(index, value);
        }
    }

    private static void setNullableLong(PreparedStatement ps, int index, Long value) throws SQLException {
        if (value == null) {
            ps.setNull(index, java.sql.Types.BIGINT);
        } else {
            ps.setLong(index, value);
        }
    }
}
