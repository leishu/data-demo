package com.demo.lake.server.batch.store;

import java.time.Instant;
import java.util.List;

/**
 * 作业日志的共享存储：执行器写入，tail / SSE 从任意服务实例读取。
 *
 * <p>本地执行器和 K8s 执行器都把日志逐行搬进状态库，BatchJobService / BatchWatcher 只认这个接口，
 * 不需要知道日志到底来自哪台机器、哪个 Pod。</p>
 */
public interface LogStore {

    /** 一行带序号的日志，seq 在同一 run 内单调递增 */
    record LogLine(long seq, String text) {
    }

    /** 追加一批行（阻塞调用，只能在虚拟线程/调度器上执行） */
    void append(String runId, List<String> lines);

    /** 最后 lines 行，按 seq 升序 */
    List<LogLine> tail(String runId, int lines);

    /** seq 之后的新行，按 seq 升序；SSE 增量推送用 */
    List<LogLine> since(String runId, long afterSeq, int limit);

    /** 清理过期日志，返回删除行数 */
    int purge(Instant before);
}
