package com.demo.lake.server.query.service;

import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.service.LakeMetrics;
import com.demo.lake.server.support.DbLocks;
import com.demo.lake.server.support.StateDbDdl;
import com.demo.lake.server.support.StateDbTables;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.util.Locale;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
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
 * <p><b>等名额的方式</b>：Postgres 下维护一条独立的非池化长连接执行 {@code LISTEN query_slot_free}，
 * {@link Permit#close()} 归还租约后发 {@code pg_notify} 唤醒本地等待者重查名额。通知可能丢失
 * （连接重连、实例重启），所以仍保留应用层 deadline 兜底；LISTEN 不可用（H2 / 连接断开）时
 * 自动退化为低频轮询。唤醒是「一次通知放一个等待者」，避免所有等待者同时打状态库造成惊群。</p>
 *
 * <p>配额由 {@link QueryService} 的 {@code *WithQuota} 方法获取/释放，Controller 不再直接接触本类；
 * 内部调用（{@code BatchHistoryService} 读运行历史、{@code GovernanceService} 读资产目录）
 * 走 {@link QueryService} 的无配额方法，不占用户额度；{@code usingWhen} 保证正常完成、出错、
 * 客户端取消都会归还名额。</p>
 *
 * <p>本类是 {@link QueryService} 的实现细节，包内可见，不对外暴露。</p>
 */
@Service
@Slf4j
class QueryQuotaService {

    private static final long WINDOW_MS = 60_000;
    /** 租约在查询超时之外再留的余量，覆盖结果集读取与调度延迟 */
    private static final long LEASE_SLACK_MS = 5_000;
    private static final String LEASES = StateDbTables.QUERY_LEASES;
    private static final String EVENTS = StateDbTables.QUERY_EVENTS;
    /** 名额释放通知的频道；只有 Postgres 有 LISTEN/NOTIFY，H2 自动走兜底轮询 */
    private static final String NOTIFY_CHANNEL = "query_slot_free";

    private static final String DELETE_OLD_EVENTS = "DELETE FROM " + EVENTS + " WHERE at_ms < ?";
    private static final String DELETE_EXPIRED_LEASES = "DELETE FROM " + LEASES + " WHERE expires_ms <= ?";
    private static final String COUNT_EVENTS = "SELECT COUNT(*) FROM " + EVENTS + " WHERE client=?";
    private static final String COUNT_LEASES = "SELECT COUNT(*) FROM " + LEASES;
    private static final String INSERT_LEASE = """
            INSERT INTO %s (permit_id, client, acquired_ms, expires_ms)
            VALUES (?,?,?,?)
            """.formatted(LEASES);
    private static final String INSERT_EVENT = "INSERT INTO " + EVENTS + " (client, at_ms) VALUES (?,?)";
    private static final String DELETE_LEASE = "DELETE FROM " + LEASES + " WHERE permit_id=?";
    private static final String NOTIFY_SQL = "SELECT pg_notify('" + NOTIFY_CHANNEL + "', '')";

    /** listener 在专用连接上读通知的间隔，与等待请求无关 */
    private static final long LISTEN_POLL_MS = 100;
    /** LISTEN 连接断开后的重连退避 */
    private static final long LISTEN_RECONNECT_MS = 1_000;
    private static final Object WAKE = new Object();

    private final LakeProperties properties;
    private final Scheduler blocking;
    private final LakeMetrics metrics;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;

    /** 本地等待队列：一次通知只唤醒队首一个，避免惊群 */
    private final Queue<Waiter> waiters = new ConcurrentLinkedQueue<>();
    /** LISTEN 连接当前可用；不可用时 acquire 自动退回低频轮询 */
    private volatile boolean listening;
    private volatile boolean postgres;
    private volatile Connection listenConn;
    private volatile Thread listenThread;
    private final AtomicBoolean listenerRunning = new AtomicBoolean();

    QueryQuotaService(LakeProperties properties, Scheduler blocking, LakeMetrics metrics,
                @Qualifier("stateJdbc") JdbcTemplate jdbc,
                @Qualifier("stateTx") TransactionTemplate tx) {
        this.properties = properties;
        this.blocking = blocking;
        this.metrics = metrics;
        this.jdbc = jdbc;
        this.tx = tx;
    }

    @PostConstruct
    void init() {
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

        String product = "unknown";
        try {
            product = jdbc.execute((ConnectionCallback<String>) conn ->
                    conn.getMetaData().getDatabaseProductName());
        } catch (DataAccessException e) {
            log.warn("读取状态库类型失败，配额唤醒退化为轮询: {}", e.getMessage());
        }
        postgres = product != null && product.toLowerCase(Locale.ROOT).contains("postgres");
        if (!postgres) {
            log.info("状态库 {} 无 LISTEN/NOTIFY，配额唤醒使用 {}ms 兜底轮询",
                    product, properties.getQuery().getQuotaFallbackPollMs());
            return;
        }

        LakeProperties.Batch.State state = properties.getBatch().getState();
        listenerRunning.set(true);
        listenThread = Thread.ofPlatform().daemon(true).name("query-slot-listener")
                .unstarted(() -> listenLoop(state.getUrl(), state.getUser(), state.getPassword()));
        listenThread.start();
    }

    @PreDestroy
    void destroy() {
        listenerRunning.set(false);
        Thread listenerThread = listenThread;
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
        Connection connection = listenConn;
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // 关闭阶段，异常无需外抛
            }
        }
    }

    /** 专用长连接上循环 LISTEN；不在任何请求线程上做 IO */
    private void listenLoop(String url, String user, String password) {
        while (listenerRunning.get()) {
            try (Connection conn = DriverManager.getConnection(url, user, password)) {
                PGConnection pg = conn.unwrap(PGConnection.class);
                try (Statement st = conn.createStatement()) {
                    st.execute("LISTEN " + NOTIFY_CHANNEL);
                    listenConn = conn;
                    listening = true;
                    log.info("查询名额通知已连接: LISTEN {}", NOTIFY_CHANNEL);
                    while (listenerRunning.get()) {
                        // pgjdbc 没有监听器 API，靠一次查询让驱动读取 socket 上的通知
                        st.execute("SELECT 1");
                        for (PGNotification notification : pg.getNotifications()) {
                            if (NOTIFY_CHANNEL.equals(notification.getName())) {
                                wakeOne();
                            }
                        }
                        Thread.sleep(LISTEN_POLL_MS);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (SQLException e) {
                if (listenerRunning.get()) {
                    log.warn("查询名额通知连接异常: {}", e.getMessage());
                }
            } finally {
                listening = false;
                listenConn = null;
                // 连接已断，忽略
                // 断线期间的通知可能丢失：让现有等待者重查一次，并改用轮询兜底
                wakeAll();
            }
            if (listenerRunning.get()) {
                try {
                    Thread.sleep(LISTEN_RECONNECT_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    /** 一次通知只放一个等待者，空队列时才丢弃；避免 N 个等待者同时重查打满状态库 */
    void wakeOne() {
        Waiter waiter;
        while ((waiter = waiters.poll()) != null) {
            if (waiter.wake()) {
                return;
            }
        }
    }

    /** 连接断开/重连成功时让所有本地等待者重查一次 */
    void wakeAll() {
        Waiter waiter;
        while ((waiter = waiters.poll()) != null) {
            waiter.wake();
        }
    }

    /** 登记一个等待者；Mono 被取消（客户端断开）时 doFinally 自动出队 */
    Mono<Object> registerWaiter() {
        return Mono.defer(() -> {
            Waiter waiter = new Waiter();
            waiters.add(waiter);
            return waiter.signal.then(Mono.just(WAKE))
                    .doFinally(sig -> waiters.remove(waiter));
        });
    }

    int pendingWaiters() {
        return waiters.size();
    }

    /** 切换唤醒方式：生产由 LISTEN 连接管理，单测用它模拟 Postgres 主路径 */
    void setListening() {
        this.listening = true;
    }

    /** 拿到名额才能执行查询；限流或超时都抛 {@link QueryQuotaExceededException} → 429 */
    Mono<Permit> acquire(String client) {
        String key = client == null || client.isBlank() ? "anonymous" : client;
        int maxRate = Math.max(1, properties.getQuery().getMaxQueriesPerMinute());
        int maxConcurrent = Math.max(1, properties.getQuery().getMaxConcurrentQueries());
        long leaseMs = Math.max(1, properties.getQuery().getTimeoutSeconds()) * 1000L + LEASE_SLACK_MS;
        String permitId = UUID.randomUUID().toString();
        long deadline = System.currentTimeMillis() + Math.max(1, properties.getQuery().getAcquireTimeoutMs());

        return Mono.defer(() -> Mono.fromCallable(
                        () -> new Attempt(tryOnce(key, maxRate, maxConcurrent, permitId, leaseMs)))
                        .subscribeOn(blocking))
                .flatMap(attempt -> {
                    String reason = attempt.reason();
                    if (reason == null) {
                        return Mono.just(new Permit(this, permitId));
                    }
                    if ("rate-limit".equals(reason)) {
                        metrics.quotaRejected("rate-limit");
                        return Mono.error(new QueryQuotaExceededException(
                                "查询太频繁：每分钟最多 " + maxRate + " 次，请稍后再试"));
                    }
                    return Mono.error(SlotBusy.INSTANCE);
                })
                .retryWhen(Retry.withThrowable(errors -> errors.concatMap(failure -> {
                    if (!(failure instanceof SlotBusy)) {
                        return Mono.error(failure); // rate-limit 等错误立即透传
                    }
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        metrics.quotaRejected("concurrency");
                        return Mono.error(concurrencyExceeded(maxConcurrent));
                    }

                    // 主路径：等通知；LISTEN 不可用时退化为低频轮询（只作兜底，不是主唤醒机制）
                    long pollMs = Math.max(1, properties.getQuery().getQuotaFallbackPollMs());
                    Mono<Object> awakened = listening
                            ? registerWaiter()
                            : Mono.delay(Duration.ofMillis(Math.min(pollMs, remaining))).then(Mono.just(WAKE));
                    Mono<Object> timeout = Mono.delay(Duration.ofMillis(remaining))
                            .then(Mono.<Object>error(concurrencyExceeded(maxConcurrent)))
                            .doOnError(e -> metrics.quotaRejected("concurrency"));
                    return Mono.firstWithSignal(awakened, timeout);
                })));
    }

    private QueryQuotaExceededException concurrencyExceeded(int maxConcurrent) {
        return new QueryQuotaExceededException(
                "并发查询已满（上限 " + maxConcurrent + "），请稍后重试");
    }

    Mono<Void> release(Permit permit) {
        return Mono.<Void>fromRunnable(permit::close).subscribeOn(blocking);
    }

    /** @return null 表示拿到名额；否则是拒绝原因（rate-limit / concurrency） */
    private String tryOnce(String client, int maxRate, int maxConcurrent, String permitId, long leaseMs) {
        return tx.execute(status -> jdbc.execute((ConnectionCallback<String>) conn -> {
            DbLocks.lock(conn, DbLocks.QUERY_QUOTA);
            long now = System.currentTimeMillis();
            jdbc.update(DELETE_OLD_EVENTS, now - WINDOW_MS);
            // 崩溃实例没来得及 close 的租约，到期后在这里顺手回收
            jdbc.update(DELETE_EXPIRED_LEASES, now);
            int usedInWindow = jdbc.queryForObject(COUNT_EVENTS, Long.class, client).intValue();
            if (usedInWindow >= maxRate) {
                status.setRollbackOnly();
                return "rate-limit";
            }
            int leased = jdbc.queryForObject(COUNT_LEASES, Long.class).intValue();
            if (leased >= maxConcurrent) {
                status.setRollbackOnly();
                return "concurrency";
            }
            jdbc.update(INSERT_LEASE,
                    new Object[]{permitId, client, now, now + leaseMs},
                    new int[]{Types.VARCHAR, Types.VARCHAR, Types.BIGINT, Types.BIGINT});
            jdbc.update(INSERT_EVENT,
                    new Object[]{client, now}, new int[]{Types.VARCHAR, Types.BIGINT});
            return null;
        }));
    }

    private void releaseSlot(String permitId) {
        try {
            tx.execute(status -> jdbc.execute((ConnectionCallback<Void>) conn -> {
                DbLocks.lock(conn, DbLocks.QUERY_QUOTA);
                jdbc.update(DELETE_LEASE, permitId);
                return null;
            }));
        } catch (DataAccessException e) {
            log.error("归还查询名额失败: {}", e.getMessage());
            return;
        }
        // 删除先提交、再通知：被唤醒的等待者重查时一定能看见已释放的名额。
        // 通知走独立短连接且 best-effort，失败不影响已经提交的归还。
        notifySlotFree();
    }

    private void notifySlotFree() {
        if (!postgres) {
            return;
        }
        try {
            jdbc.execute((ConnectionCallback<Void>) conn -> {
                try (Statement st = conn.createStatement()) {
                    st.execute(NOTIFY_SQL);
                }
                return null;
            });
        } catch (DataAccessException e) {
            log.warn("通知查询名额释放失败（等待者仍会超时兜底）: {}", e.getMessage());
        }
    }

    /** 一次查询的名额，close 多次只会归还一次 */
    static final class Permit implements AutoCloseable {
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

    /** 本地等待者：Sinks.One 保证一次通知只唤醒一个订阅者 */
    private static final class Waiter {
        final Mono<Object> signal;
        private final Sinks.One<Object> sink = Sinks.one();

        Waiter() {
            this.signal = sink.asMono();
        }

        boolean wake() {
            return sink.tryEmitValue(WAKE).isSuccess();
        }
    }

    /** 包一层再交给 fromCallable：它把 null 当成 empty，会吞掉「拿到名额」的结果 */
    private record Attempt(String reason) {
    }

    /** 内部重试信号：只有并发名额不足才重试，限流错误立即透传 */
    private static final class SlotBusy extends RuntimeException {
        static final SlotBusy INSTANCE = new SlotBusy();

        private SlotBusy() {
            super(null, null, false, false);
        }
    }
}
