package com.demo.lake.server.query.service;

import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.LakeMetrics;
import com.demo.lake.server.query.QueryQuotaExceededException;
import com.demo.lake.server.support.DbLocks;
import com.demo.lake.server.support.StateDbDdl;
import com.demo.lake.server.support.StateDbTables;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.sql.Types;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 查询配额：全局并发上限 + 单账号 60 秒滑动窗口，状态放在 Postgres。
 *
 * <p>以前信号量和窗口都在 JVM 里，多开一个实例额度就翻一倍，README 里写的“全局”并不成立。
 * 现在两个名额都放在状态库，并用 {@link DbLocks#QUERY_QUOTA} 行锁把「清理 + 计数 + 占位」
 * 包成事务；{@code acquire-timeout-ms} 内抢不到并发名额才返回 429。</p>
 *
 * <p>并发名额用的是带过期时间的租约行，而不是一个只增只减的计数器：实例被 kill 时
 * {@code usingWhen} 的清理来不及跑，租约到期后会被下一次请求顺手回收，不会永久吃掉额度。</p>
 *
 * <p>配额在 Controller 层获取/释放，所以内部调用（{@code BatchHistoryService} 读运行历史）
 * 不占用户配额；{@code usingWhen} 保证正常完成、出错、客户端取消都会归还名额。</p>
 */
@Service
@Slf4j
public class QueryQuotaService {

    private static final long WINDOW_MS = 60_000;
    /** 租约在查询超时之外再留的余量，覆盖结果集读取与调度延迟 */
    private static final long LEASE_SLACK_MS = 5_000;
    private static final String LEASES = StateDbTables.QUERY_LEASES;
    private static final String EVENTS = StateDbTables.QUERY_EVENTS;

    private final LakeProperties props;
    private final Scheduler blocking;
    private final LakeMetrics metrics;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    public QueryQuotaService(LakeProperties props, Scheduler blocking, LakeMetrics metrics,
                             @Qualifier("stateJdbc") JdbcTemplate jdbc,
                             @Qualifier("stateTx") TransactionTemplate tx) {
        this.props = props;
        this.blocking = blocking;
        this.metrics = metrics;
        this.jdbc = jdbc;
        this.tx = tx;
    }

    @PostConstruct
    public void init() {
        try {
            jdbc.execute(StateDbDdl.schemaBatch());
            jdbc.execute(StateDbDdl.queryLeases());
            jdbc.execute(StateDbDdl.queryEvents());
            try {
                for (String index : StateDbDdl.queryIndexes()) {
                    jdbc.execute(index);
                }
            } catch (DataAccessException ignored) {
                // 老版本 H2 可能不支持 IF NOT EXISTS；索引只是优化，缺了不影响正确性
            }
            DbLocks.ensure(jdbc.getDataSource(), DbLocks.QUERY_QUOTA);
            log.info("查询配额已落库: {} / {}", LEASES, EVENTS);
        } catch (DataAccessException e) {
            log.warn("查询配额表初始化失败（配额接口暂不可用）: {}", e.getMessage());
        }
    }

    /** 拿到名额才能执行查询；限流或超时都抛 {@link QueryQuotaExceededException} → 429 */
    public Mono<Permit> acquire(String client) {
        return Mono.fromCallable(() -> {
            String key = client == null || client.isBlank() ? "anonymous" : client;
            int maxRate = Math.max(1, props.getQuery().getMaxQueriesPerMinute());
            int maxConcurrent = Math.max(1, props.getQuery().getMaxConcurrentQueries());
            long leaseMs = Math.max(1, props.getQuery().getTimeoutSeconds()) * 1000L + LEASE_SLACK_MS;
            String permitId = UUID.randomUUID().toString();
            long deadline = System.currentTimeMillis() + Math.max(1, props.getQuery().getAcquireTimeoutMs());
            for (;;) {
                String reason = tryOnce(key, maxRate, maxConcurrent, permitId, leaseMs);
                if (reason == null) {
                    return new Permit(this, permitId);
                }
                if ("rate-limit".equals(reason)) {
                    metrics.quotaRejected("rate-limit");
                    throw new QueryQuotaExceededException("查询太频繁：每分钟最多 " + maxRate + " 次，请稍后再试");
                }
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    metrics.quotaRejected("concurrency");
                    throw new QueryQuotaExceededException(
                            "并发查询已满（上限 " + maxConcurrent + "），请稍后重试");
                }
                try {
                    Thread.sleep(Math.min(50, remaining));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new QueryQuotaExceededException("等待查询名额时被中断");
                }
            }
        }).subscribeOn(blocking);
    }

    public Mono<Void> release(Permit permit) {
        return Mono.<Void>fromRunnable(permit::close).subscribeOn(blocking);
    }

    /** @return null 表示拿到名额；否则是拒绝原因（rate-limit / concurrency） */
    private String tryOnce(String client, int maxRate, int maxConcurrent, String permitId, long leaseMs) {
        return tx.execute(status -> jdbc.execute((ConnectionCallback<String>) conn -> {
            DbLocks.lock(conn, DbLocks.QUERY_QUOTA);
            long now = System.currentTimeMillis();
            jdbc.update("DELETE FROM " + EVENTS + " WHERE at_ms < ?", now - WINDOW_MS);
            // 崩溃实例没来得及 close 的租约，到期后在这里顺手回收
            jdbc.update("DELETE FROM " + LEASES + " WHERE expires_ms <= ?", now);
            int usedInWindow = jdbc.queryForObject(
                    "SELECT COUNT(*) FROM " + EVENTS + " WHERE client=?", Long.class, client).intValue();
            if (usedInWindow >= maxRate) {
                status.setRollbackOnly();
                return "rate-limit";
            }
            int leased = jdbc.queryForObject("SELECT COUNT(*) FROM " + LEASES, Long.class).intValue();
            if (leased >= maxConcurrent) {
                status.setRollbackOnly();
                return "concurrency";
            }
            jdbc.update("INSERT INTO " + LEASES + " (permit_id, client, acquired_ms, expires_ms) VALUES (?,?,?,?)",
                    new Object[]{permitId, client, now, now + leaseMs},
                    new int[]{Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.BIGINT});
            jdbc.update("INSERT INTO " + EVENTS + " (client, at_ms) VALUES (?,?)",
                    new Object[]{client, now}, new int[]{Types.VARCHAR, Types.BIGINT});
            return null;
        }));
    }

    private void releaseSlot(String permitId) {
        try {
            tx.execute(status -> jdbc.execute((ConnectionCallback<Void>) conn -> {
                DbLocks.lock(conn, DbLocks.QUERY_QUOTA);
                jdbc.update("DELETE FROM " + LEASES + " WHERE permit_id=?", permitId);
                return null;
            }));
        } catch (DataAccessException e) {
            log.error("归还查询名额失败: {}", e.getMessage());
        }
    }

    /** 一次查询的名额，close 多次只会归还一次 */
    public static final class Permit implements AutoCloseable {
        private final QueryQuotaService owner;
        private final String permitId;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Permit(QueryQuotaService owner, String permitId) {
            this.owner = owner;
            this.permitId = permitId;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                owner.releaseSlot(permitId);
            }
        }
    }
}
