package com.demo.lake.server.batch.service;

import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.server.batch.model.BatchEvent;
import com.demo.lake.server.batch.model.BatchRun;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 把一次作业运行「盯」成一条 SSE 流：状态变了发 status，日志有新行发 log，跑完发 end 并关连接。
 *
 * <p>日志优先从共享 LogSink 读（执行器边跑边搬进状态库），所以请求落到哪台实例都能看到；
 * 老记录没写库时才退回本地文件游标。</p>
 *
 * <p>实现是「非阻塞节拍 + 阻塞 IO 卸载」：轮询间隔由 Reactor 的 timer 驱动，等待期间不占线程；
 * 状态查询、tail / since / 文件读取这类阻塞 IO 用 {@code subscribeOn(blocking)} 卸到虚拟线程，单次调用用完即还。
 * 取消会直接 dispose timer，异常走 onError，无需手动检查取消状态。</p>
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
        // 校验与建状态都延迟到订阅时并卸到虚拟线程：id 不存在时抛 404（自动走 onError），
        // 阻塞的状态查询也不占 Netty 事件循环；每条订阅独立持有一份 TailState。
        return Mono.fromCallable(() -> {
                    BatchRun run = jobs.get(id);
                    return new TailState(jobs, logSink, id, tail, BatchJobService.logFile(run));
                })
                .subscribeOn(blocking)
                .flatMapMany(state -> {
                    Flux<BatchEvent> first = Mono.fromCallable(state::init)
                            .subscribeOn(blocking)
                            .flatMapMany(Flux::fromIterable);

                    // repeatWhen 保证「首轮立即 poll，之后每轮间隔 POLL_MILLIS」；
                    // takeUntil 收到含 End 的那批就完成，并取消掉还没触发的下一轮 timer。
                    Flux<BatchEvent> polls = Mono.fromCallable(state::pollOnce)
                            .subscribeOn(blocking)
                            .repeatWhen(again -> again.delayElements(Duration.ofMillis(POLL_MILLIS)))
                            .takeUntil(events -> state.finished())
                            .flatMapIterable(events -> events);

                    return Flux.concat(first, polls);
                });
    }

    /** 一条流的轮询状态与事件决策；数组 holder 换成字段，语义不变 */
    static final class TailState {
        private final BatchJobService jobs;
        private final LogSink logSink;
        private final String id;
        private final int tailLines;
        private final Path logFile;
        private final LogTailCursor cursor = new LogTailCursor();

        // 响应式链保证串行访问，但每轮可能落在不同虚拟线程上，用 volatile 明确可见性
        private volatile boolean useShared;
        private volatile long seq;
        private volatile boolean localEmitted;
        private volatile BatchRun last;
        private volatile long lastEmit;
        private volatile boolean finished;

        TailState(BatchJobService jobs, LogSink logSink, String id, int tailLines, Path logFile) {
            this.jobs = jobs;
            this.logSink = logSink;
            this.id = id;
            this.tailLines = tailLines;
            this.logFile = logFile;
        }

        /** 首帧：共享库里有日志就从那里回放，否则用本地文件尾部 */
        List<BatchEvent> init() {
            List<BatchEvent> events = new ArrayList<>();
            List<LogSink.LogLine> initial = logSink.tail(id, tailLines);
            if (!initial.isEmpty()) {
                useShared = true;
                seq = initial.getLast().seq();
                addLogs(events, initial.stream().map(LogSink.LogLine::text).toList());
            } else if (logFile != null) {
                try {
                    cursor.primeTail(logFile, tailLines);
                } catch (IOException e) {
                    log.debug("预读作业日志尾部失败: {}", logFile, e);
                }
            }
            return events;
        }

        /** 一次轮询：按原顺序产出 status（状态变化时）/ log / ping / end */
        List<BatchEvent> pollOnce() {
            List<BatchEvent> events = new ArrayList<>();
            BatchRun run = jobs.get(id);
            if (last == null || !Objects.equals(last.status(), run.status())) {
                events.add(new BatchEvent.Status(run));
                last = run;
                lastEmit = System.nanoTime();
            }

            if (switchToSharedIfReady(events)) {
                lastEmit = System.nanoTime();
            }
            List<String> lines = pollLogs();
            if (!lines.isEmpty()) {
                addLogs(events, lines);
                lastEmit = System.nanoTime();
                if (!useShared) {
                    localEmitted = true;
                }
            }

            // 未知/脏状态（BatchRunStatus.from 返回 null）也按终态收口：继续等只会让 SSE 永远挂起；
            // 状态库里那条 RUNNING 会由对账按心跳超时收掉，不需要这里替它编造状态。
            BatchRunStatus status = run.status();
            if (status == null || status.isTerminal()) {
                // 终态前再抽干一次，避免最后几行日志被 end 事件抢跑
                switchToSharedIfReady(events);
                addLogs(events, pollLogs());
                events.add(new BatchEvent.End(run));
                finished = true;
                return events;
            }
            if (System.nanoTime() - lastEmit > PING_EVERY.toNanos()) {
                events.add(new BatchEvent.Ping());
                lastEmit = System.nanoTime();
            }
            return events;
        }

        boolean finished() {
            return finished;
        }

        /**
         * 首帧共享日志为空时不能一锤定音：作业刚排队/刚启动，或请求落到非执行实例，后续日志才会出现。
         * 每轮惰性重探，一旦共享库有行就切过去；若此前已经从本机文件发过日志，则只补增量不回放历史，
         * 避免同一批日志在页面上出现两遍。
         *
         * @return 是否向流里发过历史日志
         */
        private boolean switchToSharedIfReady(List<BatchEvent> events) {
            if (useShared) {
                return false;
            }
            List<LogSink.LogLine> appeared = logSink.tail(id, tailLines);
            if (appeared.isEmpty()) {
                return false;
            }
            useShared = true;
            seq = appeared.getLast().seq();
            if (localEmitted) {
                return false;
            }
            addLogs(events, appeared.stream().map(LogSink.LogLine::text).toList());
            return true;
        }

        /** seq 是本条流已读到的最大序号，随实例更新，互不影响 */
        private List<String> pollLogs() {
            if (useShared) {
                List<LogSink.LogLine> fresh = logSink.since(id, seq, MAX_LINES_PER_POLL);
                if (!fresh.isEmpty()) {
                    seq = fresh.getLast().seq();
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

        private static void addLogs(List<BatchEvent> events, List<String> lines) {
            if (!lines.isEmpty()) {
                events.add(new BatchEvent.Log(lines));
            }
        }
    }
}
