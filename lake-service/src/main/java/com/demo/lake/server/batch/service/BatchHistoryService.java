package com.demo.lake.server.batch.service;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.batch.model.RunsView;
import com.demo.lake.server.query.PrestoUnavailableException;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.service.QueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

/**
 * 读湖表 {@code ops.batch_runs} 里的运行历史（由 BatchApp 在作业结束时写入）。
 *
 * <p>这是旁路数据源，三条降级规则：</p>
 * <ol>
 *   <li>表不存在（从没跑过 init）→ 空列表 + NOT_INITIALIZED，不去执行注定报错的查询；</li>
 *   <li>Presto 连不上 / 查询失败 → 空列表 + UNAVAILABLE/ERROR，只记 warn；</li>
 *   <li>探测结果缓存 {@value #PROBE_TTL_SECONDS}s：页面每 6s 刷一次列表，
 *       不能每次都去问 information_schema，也不能让「刚跑完 init」等太久。</li>
 * </ol>
 *
 * <p>读列表的结果本身也缓存 {@value #CACHE_TTL_SECONDS}s 并单飞：多开几个页面时，
 * 6s 的轮询会被放大成 N 倍，缓存把同一时间窗内的重复读合并成一次 Presto 查询。</p>
 *
 * <p>方法都是阻塞 JDBC，调用方保证跑在 blocking 调度器上（{@link BatchJobService} 负责）。</p>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BatchHistoryService {

    private static final long PROBE_TTL_SECONDS = 15;
    private static final long PROBE_TTL_NANOS = TimeUnit.SECONDS.toNanos(PROBE_TTL_SECONDS);
    /** 轮询缓存：略小于页面 6s 节拍，同一拍里只回源一次；内存记录实时合并，不受此缓存影响 */
    private static final long CACHE_TTL_SECONDS = 5;
    private static final long CACHE_TTL_NANOS = TimeUnit.SECONDS.toNanos(CACHE_TTL_SECONDS);
    /** run id 来自路径变量，拼进 SQL 前必须卡死字符集 */
    private static final Pattern RUN_ID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._:-]{0,79}");
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final int MAX_LIMIT = 100;

    private final QueryService queries;
    private final PlatformConfig cfg;

    /** 虚拟线程上用 ReentrantLock 而不是 synchronized：锁内是阻塞 JDBC，synchronized 会 pin 住 carrier */
    private final ReentrantLock cacheLock = new ReentrantLock();
    private volatile Snapshot cached;

    private volatile long probedAtNanos;
    private volatile boolean present;

    public record Result(List<BatchRun> runs, RunsView.History history) {
    }

    /** 最近一次读到的列表；limit 变了就重查（页面固定 20，实际只有一种） */
    private record Snapshot(long atNanos, int limit, Result result) {
    }

    /** 最近 limit 条历史；任何失败都降级成空列表 + 状态说明 */
    public Result recent(int limit) {
        int n = Math.max(1, Math.min(limit, MAX_LIMIT));
        Result hit = fresh(n);
        if (hit != null) {
            return hit;
        }
        // 多页面同时到：只让第一个回源，其余等它填完缓存直接复用
        cacheLock.lock();
        try {
            hit = fresh(n);
            if (hit != null) {
                return hit;
            }
            Result loaded = load(n);
            cached = new Snapshot(System.nanoTime(), n, loaded);
            return loaded;
        } finally {
            cacheLock.unlock();
        }
    }

    private Result fresh(int limit) {
        Snapshot snapshot = cached;
        if (snapshot == null || snapshot.limit() != limit) {
            return null;
        }
        return System.nanoTime() - snapshot.atNanos() < CACHE_TTL_NANOS ? snapshot.result() : null;
    }

    private Result load(int n) {
        String table = table();
        try {
            if (!present()) {
                return new Result(List.of(), new RunsView.History(RunsView.History.NOT_INITIALIZED,
                        "运行历史表 " + table + " 还没建（先跑一次 init），当前只显示本进程的记录", table));
            }
            return new Result(List.copyOf(select(" order by started_at desc limit " + n)),
                    new RunsView.History(RunsView.History.OK, null, table));
        } catch (PrestoUnavailableException e) {
            invalidateProbe();
            log.warn("读运行历史失败（Presto 不可用）: {}", e.getMessage());
            return new Result(List.of(), new RunsView.History(RunsView.History.UNAVAILABLE,
                    "连不上 Presto，当前只显示本进程的运行记录", table));
        } catch (RuntimeException e) {
            invalidateProbe();
            log.warn("读运行历史失败: {}", e.getMessage());
            return new Result(List.of(), new RunsView.History(RunsView.History.ERROR,
                    "读运行历史失败：" + e.getMessage(), table));
        }
    }

    /** 作业状态变化后调用：下次读列表立刻回源，也重新探测表在不在 */
    public void invalidate() {
        cached = null;
        invalidateProbe();
    }

    /** 服务重启后按 id 找回一条历史；找不到或读不到都返回 null，由调用方给 404 */
    public BatchRun find(String id) {
        if (id == null || !RUN_ID.matcher(id).matches()) {
            return null;
        }
        try {
            if (!present()) {
                return null;
            }
            List<BatchRun> hits = select(" where run_id = '" + id + "' limit 1");
            return hits.isEmpty() ? null : hits.get(0);
        } catch (RuntimeException e) {
            invalidateProbe();
            log.warn("按 id 读运行历史失败 {}: {}", id, e.getMessage());
            return null;
        }
    }

    private List<BatchRun> select(String suffix) {
        // 时间列取 epoch 毫秒：Presto 的 timestamp 不带时区，cast 成 varchar 或驱动转成
        // java.sql.Timestamp 都会按会话时区重排（同一行在 UTC 会话与 Asia/Shanghai 会话下差 8 小时），
        // 只有 to_unixtime 这种绝对时刻换算与连接时区无关。
        String sql = "select run_id, job, command, status, exit_code,"
                + " cast(round(to_unixtime(started_at) * 1000) as bigint) as started_ms,"
                + " cast(round(to_unixtime(finished_at) * 1000) as bigint) as finished_ms,"
                + " duration_ms, log_file, message from " + table() + suffix;
        QueryResult result = queries.executeQuiet(sql, null);
        List<BatchRun> runs = new ArrayList<>(result.rows().size());
        for (List<String> row : result.rows()) {
            runs.add(toRun(row));
        }
        return runs;
    }

    /** 表在不在：问 Presto 自己的系统表，不额外维护一份表清单 */
    private boolean present() {
        long now = System.nanoTime();
        long at = probedAtNanos;
        if (at != 0 && now - at < PROBE_TTL_NANOS) {
            return present;
        }
        String sql = "select count(*) from " + catalog() + ".information_schema.tables"
                + " where table_schema = '" + LakeTables.OPS_SCHEMA + "'"
                + " and table_name = '" + LakeTables.BATCH_RUNS + "'";
        QueryResult result = queries.executeQuiet(sql, null);
        boolean found = !result.rows().isEmpty() && !"0".equals(result.rows().get(0).get(0));
        present = found;
        probedAtNanos = now;
        return found;
    }

    private void invalidateProbe() {
        // 表可能被删了、Presto 可能刚重启：下次调用重新探测
        probedAtNanos = 0;
    }

    private String table() {
        return catalog() + "." + LakeTables.OPS_SCHEMA + "." + LakeTables.BATCH_RUNS;
    }

    private String catalog() {
        String catalog = cfg.catalogName();
        if (catalog == null || !IDENTIFIER.matcher(catalog).matches()) {
            throw new IllegalStateException("非法 catalog 名: " + catalog);
        }
        return catalog;
    }

    private static BatchRun toRun(List<String> row) {
        return new BatchRun(
                text(row, 0),
                text(row, 1),
                text(row, 2),
                text(row, 3),
                toInt(text(row, 4)),
                toInstantMillis(text(row, 5)),
                toInstantMillis(text(row, 6)),
                toLong(text(row, 7)),
                text(row, 8),
                text(row, 9));
    }

    private static String text(List<String> row, int index) {
        return index < row.size() ? row.get(index) : null;
    }

    /**
     * 入参是 SQL 里算出来的 epoch 毫秒。不能直接取 timestamp：无论 cast 成字符串还是驱动转成
     * java.sql.Timestamp，都会按会话时区/ JVM 时区重排，同一时刻在不同时区下得到不同值。
     */
    private static Instant toInstantMillis(String text) {
        Long millis = toLong(text);
        return millis == null ? null : Instant.ofEpochMilli(millis);
    }

    private static Integer toInt(String text) {
        try {
            return text == null || text.isBlank() ? null : Integer.valueOf(text.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long toLong(String text) {
        try {
            return text == null || text.isBlank() ? null : Long.valueOf(text.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
