package com.demo.lake.servier.batch.service;

import com.demo.lake.servier.batch.model.BatchEvent;
import com.demo.lake.servier.batch.model.BatchRun;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * 把一次作业运行「盯」成一条 SSE 流：状态变了发 status，日志有新行发 log，跑完发 end 并关连接。
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

    private final BatchJobService jobs;
    private final Scheduler blocking;

    public Flux<BatchEvent> watch(String id) {
        // defer：id 不存在时在这里抛 404，而不是先建一条空流再报错
        return Flux.defer(() -> {
            jobs.get(id);
            return Flux.<BatchEvent>create(sink -> loop(id, sink), FluxSink.OverflowStrategy.BUFFER)
                    .subscribeOn(blocking);
        });
    }

    private void loop(String id, FluxSink<BatchEvent> sink) {
        LogTailCursor cursor = new LogTailCursor();
        Path logFile = Paths.get(jobs.get(id).logFile());
        BatchRun last = null;
        long lastEmit = 0;
        while (!sink.isCancelled()) {
            BatchRun run = jobs.get(id);
            if (last == null || !Objects.equals(last.status(), run.status())) {
                sink.next(new BatchEvent.Status(run));
                last = run;
                lastEmit = System.nanoTime();
            }
            try {
                List<String> lines = cursor.poll(logFile, MAX_LINES_PER_POLL);
                if (!lines.isEmpty()) {
                    sink.next(new BatchEvent.Log(lines));
                    lastEmit = System.nanoTime();
                }
            } catch (Exception e) {
                log.debug("读取作业日志失败: {}", logFile, e);
                sink.next(new BatchEvent.Log(List.of("（日志读取失败：" + e.getMessage() + "）")));
            }
            if (!BatchRun.RUNNING.equals(run.status())) {
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
}
