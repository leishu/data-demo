package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 批作业的运行态存储：服务不再把 RUNNING 放在自己的 HashMap 里。
 *
 * <p>终态归档仍由 BatchApp 写进湖表 {@code ops.batch_runs}；这里是「服务侧运行态」，
 * 多实例共享、重启不丢。恢复时由 BatchJobService 拿湖表终态来对账。</p>
 */
public interface RunStateStore {

    /** owner 是提交作业的服务实例 ID，心跳只由它续期 */
    void insert(BatchRun run, String owner);

    void finish(BatchRun run);

    Optional<BatchRun> find(String id);

    List<BatchRun> recent(int limit);

    /** 刷新本实例名下所有 RUNNING 记录的心跳 */
    void heartbeat(String owner, Instant now);

    /** 心跳超时的 RUNNING 记录，等待和湖表终态对账 */
    List<BatchRun> staleRunning(Instant before);

    /** 对账也找不到终态：按「实例失联」落终态，别让 RUNNING 变成永久脏数据 */
    void markLost(String id, String message, Instant now);

    /** 幂等键 → run id；重复提交直接返回原运行记录 */
    Optional<BatchRun> findByIdempotencyKey(String key);

    void recordIdempotency(String key, String runId);

    /** 当前 RUNNING 数量，用于全局并发闸门 */
    int runningCount();

    /** 当前 QUEUED 数量，作为队列深度指标 */
    int queuedCount();

    /** 原子地把最早的一条 QUEUED 改成 RUNNING，并归本实例所有；没有可认领的返回 null */
    BatchRun claimNext(String owner);

    /** 记下外部执行器句柄（本地进程 PID 或 K8s Job 名），便于排障/取消 */
    void setHandle(String runId, String handle);
}
