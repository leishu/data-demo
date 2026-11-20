package com.demo.lake.server.query.service;

import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.LakeMetrics;
import com.demo.lake.server.query.QueryQuotaExceededException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 查询配额：全局并发上限 + 单账号滑动窗口限流。
 *
 * <p>Hikari 的 pool-size 只能限制「同时连着 Trino 的数量」，挡不住 N 个页面同时排队等连接；
 * 这里用信号量把排队也卡住，等不到名额就直接 429，避免慢查询把整个服务拖死。</p>
 *
 * <p>配额在 Controller 层获取/释放，所以内部调用（{@code BatchHistoryService} 读运行历史）
 * 不占用户配额；{@code usingWhen} 保证正常完成、出错、客户端取消都会归还名额。</p>
 */
@Service
@Slf4j
public class QueryQuotaService {

    private static final long WINDOW_MS = 60_000;
    /** 超过这个数量的账号窗口才做一次清理，避免每次请求都遍历 */
    private static final int CLEANUP_THRESHOLD = 512;

    private final LakeProperties props;
    private final Scheduler blocking;
    private final LakeMetrics metrics;
    private final Semaphore slots;
    private final ConcurrentHashMap<String, Window> windows = new ConcurrentHashMap<>();

    public QueryQuotaService(LakeProperties props, Scheduler blocking, LakeMetrics metrics) {
        this.props = props;
        this.blocking = blocking;
        this.metrics = metrics;
        this.slots = new Semaphore(Math.max(1, props.getQuery().getMaxConcurrentQueries()), true);
    }

    /** 拿到名额才能执行查询；限流或超时都抛 {@link QueryQuotaExceededException} → 429 */
    public Mono<Permit> acquire(String client) {
        return Mono.fromCallable(() -> {
            checkRate(client);
            int timeout = Math.max(1, props.getQuery().getAcquireTimeoutMs());
            boolean acquired;
            try {
                acquired = slots.tryAcquire(timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new QueryQuotaExceededException("等待查询名额时被中断");
            }
            if (!acquired) {
                metrics.quotaRejected("concurrency");
                throw new QueryQuotaExceededException(
                        "并发查询已满（上限 " + props.getQuery().getMaxConcurrentQueries() + "），请稍后重试");
            }
            return new Permit(slots);
        }).subscribeOn(blocking);
    }

    public Mono<Void> release(Permit permit) {
        return Mono.fromRunnable(permit::close);
    }

    private void checkRate(String client) {
        int max = Math.max(1, props.getQuery().getMaxQueriesPerMinute());
        long now = System.currentTimeMillis();
        Window window = windows.computeIfAbsent(client == null ? "anonymous" : client, k -> new Window());
        if (!window.tryRecord(now, max)) {
            metrics.quotaRejected("rate-limit");
            throw new QueryQuotaExceededException("查询太频繁：每分钟最多 " + max + " 次，请稍后再试");
        }
        if (windows.size() > CLEANUP_THRESHOLD) {
            windows.forEach((key, w) -> {
                if (w.expired(now)) {
                    windows.remove(key, w);
                }
            });
            log.debug("清理空闲查询配额窗口，剩余 {}", windows.size());
        }
    }

    /** 一次查询的名额，close 多次只会归还一次 */
    public static final class Permit implements AutoCloseable {
        private final Semaphore slots;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Permit(Semaphore slots) {
            this.slots = slots;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                slots.release();
            }
        }
    }

    /** 单账号的 60 秒滑动窗口；只放时间戳，不存查询结果 */
    private static final class Window {
        private final ArrayDeque<Long> times = new ArrayDeque<>();

        synchronized boolean tryRecord(long now, int max) {
            prune(now);
            if (times.size() >= max) {
                return false;
            }
            times.addLast(now);
            return true;
        }

        synchronized boolean expired(long now) {
            prune(now);
            return times.isEmpty();
        }

        private void prune(long now) {
            while (!times.isEmpty() && now - times.peekFirst() >= WINDOW_MS) {
                times.pollFirst();
            }
        }
    }
}
