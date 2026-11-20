package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchEvent;
import com.demo.lake.server.batch.model.BatchRun;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * 把一次作业运行「盯」成一条 SSE 流：状态变了发 status，日志有新行发 log，跑完发 end 并关连接。
 *
 * <p>日志优先从共享 LogSink 读（执行器边跑边搬进状态库），所以请求落到哪台实例都能看到；
 * 老记录没写库时才退回本地文件游标。</p>
 *
 * <p>实现是「虚拟线程里 sleep + 轮询」的朴素循环，没走 {@code Flux.interval}：日志读取是阻塞 IO，
 * 用定时器驱动反而要额外处理背压与线程切换。一条流一个虚拟线程，代价可以忽略。</p>
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class BatchWatcher {

    private static final long POLL_MILLIS = 500;
    private static final Duration PING_EVERY = Duration.ofSeconds(10);
    private static final int MAX_LINES_PER_POLL = 200;
    /** 首帧只回放这么多历史行：长日志从头刷一遍会让详情面板等好几秒 */
    private static final int MAX_TAIL_LINES = 1000;

    private final BatchJobService jobs;
    private final LogSink logSink;
    private final Scheduler blocking;

    public Flux<BatchEvent> watch(String id, int tailLines) {
        int tail = Math.clamp(tailLines, 1, MAX_TAIL_LINES);
        // defer：id 不存在时在这里抛 404，而不是先建一条空流再报错
        return Flux.defer(() -> {
            jobs.get(id);
            return Flux.<BatchEvent>create(sink -> loop(id, tail, sink), FluxSink.OverflowStrategy.BUFFER)
                    .subscribeOn(blocking);
        });
    }

    private void loop(String id, int tailLines, FluxSink<BatchEvent> sink) {
        LogTailCursor cursor = new LogTailCursor();
        Path logFile = BatchJobService.logFile(jobs.get(id));

        // 首帧：共享库里有日志就从那里回放，否则用本地文件尾部
        long[] seqHolder = new long[1];
        List<LogSink.LogLine> initial = logSink.tail(id, tailLines);
        boolean useShared = !initial.isEmpty();
        if (useShared) {
            seqHolder[0] = initial.getLast().seq();
            emit(sink, initial.stream().map(LogSink.LogLine::text).toList());
        } else if (logFile != null) {
            try {
                cursor.primeTail(logFile, tailLines);
            } catch (IOException e) {
                log.debug("预读作业日志尾部失败: {}", logFile, e);
            }
        }

        BatchRun last = null;
        long lastEmit = 0;
        while (!sink.isCancelled()) {
            BatchRun run = jobs.get(id);
            if (last == null || !Objects.equals(last.status(), run.status())) {
                sink.next(new BatchEvent.Status(run));
                last = run;
                lastEmit = System.nanoTime();
            }

            List<String> lines = pollLogs(id, useShared, cursor, logFile, seqHolder);
            if (!lines.isEmpty()) {
                emit(sink, lines);
                lastEmit = System.nanoTime();
            }

            if (!BatchRun.RUNNING.equals(run.status()) && !BatchRun.QUEUED.equals(run.status())) {
                // 终态前再抽干一次，避免最后几行日志被 end 事件抢跑
                List<String> finalLines = pollLogs(id, useShared, cursor, logFile, seqHolder);
                if (!finalLines.isEmpty()) {
                    emit(sink, finalLines);
                }
                sink.next(new BatchEvent.End(run));
                sink.complete();
                return;
            }
            if (System.nanoTime() - lastEmit > PING_EVERY.toNanos()) {
                sink.next(new BatchEvent.Ping());
                lastEmit = System.nanoTime();
            }
            try {
                Thread.sleep(POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                sink.complete();
                return;
            }
        }
    }

    /** seqHolder[0] 是本条流已读到的最大序号，按引用更新，互不影响 */
    private List<String> pollLogs(String id, boolean useShared, LogTailCursor cursor, Path logFile, long[] seqHolder) {
        if (useShared) {
            List<LogSink.LogLine> fresh = logSink.since(id, seqHolder[0], MAX_LINES_PER_POLL);
            if (!fresh.isEmpty()) {
                seqHolder[0] = fresh.getLast().seq();
                return fresh.stream().map(LogSink.LogLine::text).toList();
            }
            return List.of();
        }
        if (logFile == null) {
            return List.of();
        }
        try {
            return cursor.poll(logFile, MAX_LINES_PER_POLL);
        } catch (Exception e) {
            log.debug("读取作业日志失败: {}", logFile, e);
            return List.of("（日志读取失败：" + e.getMessage() + "）");
        }
    }

    private static void emit(FluxSink<BatchEvent> sink, List<String> lines) {
        if (!lines.isEmpty()) {
            sink.next(new BatchEvent.Log(lines));
        }
    }
}
