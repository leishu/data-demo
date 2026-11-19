package com.demo.lake.server.controller;

import com.demo.lake.server.batch.model.BatchEvent;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.batch.model.RunsView;
import com.demo.lake.server.batch.service.BatchJobService;
import com.demo.lake.server.batch.service.BatchWatcher;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.List;

/** 批作业编排接口 */
@RestController
@RequestMapping("/api/batch")
@RequiredArgsConstructor
public class BatchController {

    private final BatchJobService jobService;
    private final BatchWatcher watcher;
    private final Scheduler blocking;

    public record RunDetail(BatchRun run, List<String> tail) {
    }

    /** 202：作业已提交，进度看 /api/batch/runs/{id}/events（SSE）或 /{id} */
    @PostMapping("/{job}")
    public Mono<ResponseEntity<BatchRun>> submit(@PathVariable String job) {
        // 建日志目录 + 起子进程都是阻塞动作，扔到 blocking 调度器上
        return Mono.fromCallable(() -> jobService.submit(job))
                .subscribeOn(blocking)
                .map(run -> ResponseEntity.status(HttpStatus.ACCEPTED).body(run));
    }

    @GetMapping("/runs")
    public Mono<RunsView> runs(@RequestParam(defaultValue = "20") int limit) {
        return Mono.fromCallable(() -> jobService.list(limit)).subscribeOn(blocking);
    }

    @GetMapping("/runs/{id}")
    public Mono<RunDetail> detail(@PathVariable String id,
                                  @RequestParam(defaultValue = "40") int tail) {
        // 日志尾部要读文件，同样别在事件循环上做
        return Mono.fromCallable(() -> new RunDetail(jobService.get(id), jobService.tail(id, tail)))
                .subscribeOn(blocking);
    }

    /**
     * 作业监控流：status（状态变化）/ log（日志增量）/ ping（心跳）/ end（终态，随后关闭）。
     * 前端一条 EventSource 顶掉原来的 2.5s 轮询；tail 是首帧回放的历史日志行数。
     */
    @GetMapping(value = "/runs/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<BatchEvent>> events(@PathVariable String id,
                                                   @RequestParam(defaultValue = "80") int tail) {
        return watcher.watch(id, tail)
                .map(ev -> ServerSentEvent.builder(ev).event(ev.type()).build());
    }
}
