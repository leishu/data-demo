package com.demo.lake.server.batch.store;

import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.server.batch.model.BatchRun;
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 用 Postgres 存运行态（复用 infra 的元数据库；数据库访问走 `spring-jdbc` 的
 * `JdbcTemplate` / `TransactionTemplate`，不引入新的数据库组件）。
 *
 * <p>表在服务启动时幂等创建；SQL 全是固定语句 + 占位符，没有 SQL 注入面。
 * 每条记录带 owner / heartbeat_ms：心跳只由「执行器还活着」的 run 续期，失联的对账后落终态。</p>
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class JdbcRunStateStore implements RunStateStore {

    private static final String TABLE = "lake_batch.batch_runs";
    /** SQL 里的状态字面量统一取自枚举，避免与 Java 侧定义漂移 */
    private static final String S_RUNNING = BatchRunStatus.RUNNING.name();
    private static final String S_QUEUED = BatchRunStatus.QUEUED.name();
    private static final String S_FAILED = BatchRunStatus.FAILED.name();

    private static final String SELECT = "SELECT run_id, job, command, status, exit_code, started_ms,"
            + " finished_ms, duration_ms, log_file, message FROM " + TABLE;

    private static final String INSERT_RUN = "INSERT INTO " + TABLE + " (run_id, job, command, status, exit_code,"
            + " started_ms, finished_ms, duration_ms, log_file, message, owner, heartbeat_ms, updated_ms)"
            + " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)";

    // insert 各列的类型：null 值按列类型 setNull（默认推断会丢失列类型，跨 H2/Postgres 不稳）
    private static final int[] INSERT_RUN_TYPES = new int[]{
            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER,
            Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.VARCHAR, Types.VARCHAR,
            Types.VARCHAR, Types.BIGINT, Types.BIGINT};

    @Qualifier("stateJdbc")
    private final JdbcTemplate jdbc;
    @Qualifier("stateTx")
    private final TransactionTemplate tx;

    @PostConstruct
    public void init() {
        try {
            jdbc.execute("CREATE SCHEMA IF NOT EXISTS lake_batch");
            jdbc.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (\n"
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
            jdbc.execute("CREATE INDEX IF NOT EXISTS idx_lake_batch_runs_started ON " + TABLE + "(started_ms DESC)");
            // 老库升级：幂等/调度引入的新列按需补上（Postgres 与 H2 都支持 information_schema）
            ensureColumn("handle", "handle VARCHAR(255)");
            jdbc.execute("CREATE TABLE IF NOT EXISTS lake_batch.idempotency (\n"
                    + "  idem_key VARCHAR(128) PRIMARY KEY,\n"
                    + "  run_id VARCHAR(80) NOT NULL\n"
                    + ")");
            DbLocks.ensure(jdbc.getDataSource(), DbLocks.SUBMIT, DbLocks.DISPATCH);
            log.info("运行状态表就绪: {}", TABLE);
        } catch (DataAccessException e) {
            // 不阻止启动：infra 没起时只读查询还能用，批作业接口会返回 503
            log.warn("运行状态表初始化失败（批作业接口暂不可用）: {}", e.getMessage());
        }
    }

    private void ensureColumn(String name, String ddl) {
        List<Long> found = jdbc.query(
                "SELECT 1 FROM information_schema.columns WHERE LOWER(table_schema)='lake_batch'"
                        + " AND LOWER(table_name)='batch_runs' AND LOWER(column_name)=?",
                (rs, n) -> rs.getLong(1),
                new Object[]{name});
        if (found.isEmpty()) {
            jdbc.execute("ALTER TABLE " + TABLE + " ADD COLUMN " + ddl);
        }
    }

    @Override
    public void insert(BatchRun run, String owner) {
        jdbc.update(INSERT_RUN, bindRun(run, owner), INSERT_RUN_TYPES);
    }

    @Override
    public String insertWithIdempotency(BatchRun run, String owner, String key) {
        return tx.execute(status -> jdbc.execute((ConnectionCallback<String>) conn -> {
            // 全局提交锁：同一时刻只有一个请求在做「查键 + 插记录 + 登记键」
            DbLocks.lock(conn, DbLocks.SUBMIT);
            List<String> existing = jdbc.query(
                    "SELECT run_id FROM lake_batch.idempotency WHERE idem_key=?",
                    (rs, n) -> rs.getString(1),
                    new Object[]{key});
            if (!existing.isEmpty()) {
                return existing.getFirst();
            }
            jdbc.update(INSERT_RUN, bindRun(run, owner), INSERT_RUN_TYPES);
            jdbc.update("INSERT INTO lake_batch.idempotency (idem_key, run_id) VALUES (?,?)", key, run.id());
            return run.id();
        }));
    }

    private static Object[] bindRun(BatchRun run, String owner) {
        long now = System.currentTimeMillis();
        return new Object[]{
                run.id(),
                run.job(),
                run.command(),
                run.status() == null ? null : run.status().name(),
                run.exitCode(),
                run.startedAt().toEpochMilli(),
                run.finishedAt() == null ? null : run.finishedAt().toEpochMilli(),
                run.durationMs(),
                run.logFile(),
                run.message(),
                owner,
                now,
                now,
        };
    }

    @Override
    public void finish(BatchRun run) {
        jdbc.update("UPDATE " + TABLE + " SET status=?, exit_code=?, finished_ms=?, duration_ms=?,"
                        + " message=?, updated_ms=? WHERE run_id=?",
                new Object[]{run.status() == null ? null : run.status().name(), run.exitCode(),
                        run.finishedAt() == null ? System.currentTimeMillis() : run.finishedAt().toEpochMilli(),
                        run.durationMs(), run.message(), System.currentTimeMillis(), run.id()},
                new int[]{Types.VARCHAR, Types.INTEGER, Types.BIGINT, Types.BIGINT, Types.VARCHAR,
                        Types.BIGINT, Types.VARCHAR});
    }

    @Override
    public Optional<BatchRun> find(String id) {
        List<BatchRun> rows = jdbc.query(SELECT + " WHERE run_id=?",
                (rs, n) -> map(rs),
                new Object[]{id});
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }

    @Override
    public List<BatchRun> recent(int limit) {
        return jdbc.query(SELECT + " ORDER BY started_ms DESC LIMIT ?",
                (rs, n) -> map(rs),
                new Object[]{limit});
    }

    @Override
    public void heartbeat(String runId, Instant now) {
        jdbc.update("UPDATE " + TABLE + " SET heartbeat_ms=?, updated_ms=? WHERE run_id=? AND status='" + S_RUNNING + "'",
                new Object[]{now.toEpochMilli(), now.toEpochMilli(), runId},
                new int[]{Types.BIGINT, Types.BIGINT, Types.VARCHAR});
    }

    @Override
    public List<BatchRun> staleRunning(Instant before) {
        return jdbc.query(SELECT + " WHERE status='" + S_RUNNING + "' AND heartbeat_ms < ? ORDER BY started_ms",
                (rs, n) -> map(rs),
                new Object[]{before.toEpochMilli()});
    }

    @Override
    public void markLost(String id, String message, Instant now) {
        jdbc.update("UPDATE " + TABLE + " SET status='" + S_FAILED + "', exit_code=1, finished_ms=?,"
                        + " duration_ms=? - started_ms, message=?, updated_ms=? WHERE run_id=? AND status='" + S_RUNNING + "'",
                new Object[]{now.toEpochMilli(), now.toEpochMilli(), message, now.toEpochMilli(), id},
                new int[]{Types.BIGINT, Types.BIGINT, Types.VARCHAR, Types.BIGINT, Types.VARCHAR});
    }

    @Override
    public Optional<BatchRun> findByIdempotencyKey(String key) {
        List<String> ids = jdbc.query("SELECT run_id FROM lake_batch.idempotency WHERE idem_key=?",
                (rs, n) -> rs.getString(1),
                new Object[]{key});
        return ids.isEmpty() ? Optional.empty() : find(ids.getFirst());
    }

    @Override
    public void recordIdempotency(String key, String runId) {
        if (findByIdempotencyKey(key).isPresent()) {
            return;
        }
        try {
            jdbc.update("INSERT INTO lake_batch.idempotency (idem_key, run_id) VALUES (?,?)", key, runId);
        } catch (RuntimeException e) {
            // 并发重复提交：别人先插了，按已存在的记录处理
            log.debug("幂等键已存在 {}: {}", key, e.getMessage());
        }
    }

    @Override
    public int runningCount() {
        return countByStatus(BatchRunStatus.RUNNING);
    }

    @Override
    public int queuedCount() {
        return countByStatus(BatchRunStatus.QUEUED);
    }

    private int countByStatus(BatchRunStatus status) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM " + TABLE + " WHERE status=?", Long.class, status.name()).intValue();
    }

    @Override
    public BatchRun claimNext(String owner) {
        return tx.execute(status -> claimOnConnection(owner));
    }

    @Override
    public BatchRun claimNextIfSlotAvailable(String owner, int maxConcurrent) {
        return tx.execute(status -> {
            // 名额检查 + 认领在同一把全局行锁内完成，跨实例也不会一起超员
            return jdbc.execute((ConnectionCallback<BatchRun>) conn -> {
                DbLocks.lock(conn, DbLocks.DISPATCH);
                if (jdbc.queryForObject("SELECT COUNT(*) FROM " + TABLE + " WHERE status='" + S_RUNNING + "'", Long.class)
                        >= maxConcurrent) {
                    status.setRollbackOnly();
                    return null;
                }
                return claimOnConnection(owner);
            });
        });
    }

    private BatchRun claimOnConnection(String owner) {
        List<String> ids = jdbc.query("SELECT run_id FROM " + TABLE + " WHERE status='" + S_QUEUED + "' ORDER BY started_ms LIMIT 1",
                (rs, n) -> rs.getString(1));
        if (ids.isEmpty()) {
            return null;
        }
        String id = ids.getFirst();
        long now = System.currentTimeMillis();
        int changed = jdbc.update("UPDATE " + TABLE
                + " SET status='" + S_RUNNING + "', owner=?, heartbeat_ms=?, updated_ms=?"
                + " WHERE run_id=? AND status='" + S_QUEUED + "'",
                new Object[]{owner, now, now, id},
                new int[]{Types.VARCHAR, Types.BIGINT, Types.BIGINT, Types.VARCHAR});
        // 多实例可能同时看到同一个候选；只有抢到 update 的那个实例真正执行
        if (changed == 0) {
            return null;
        }
        List<BatchRun> rows = jdbc.query(SELECT + " WHERE run_id=?",
                (rs, n) -> map(rs),
                new Object[]{id});
        return rows.isEmpty() ? null : rows.getFirst();
    }

    @Override
    public void setHandle(String runId, String handle) {
        jdbc.update("UPDATE " + TABLE + " SET handle=?, updated_ms=? WHERE run_id=?",
                new Object[]{handle, System.currentTimeMillis(), runId},
                new int[]{Types.VARCHAR, Types.BIGINT, Types.VARCHAR});
    }

    private static BatchRun map(ResultSet rs) throws SQLException {
        Long finished = (Long) rs.getObject("finished_ms");
        Long duration = (Long) rs.getObject("duration_ms");
        Integer exit = (Integer) rs.getObject("exit_code");
        return new BatchRun(
                rs.getString("run_id"),
                rs.getString("job"),
                rs.getString("command"),
                BatchRunStatus.from(rs.getString("status")),
                exit,
                Instant.ofEpochMilli(rs.getLong("started_ms")),
                finished == null ? null : Instant.ofEpochMilli(finished),
                duration,
                rs.getString("log_file"),
                rs.getString("message"));
    }
}
