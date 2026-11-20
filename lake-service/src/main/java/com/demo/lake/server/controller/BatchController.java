package com.demo.lake.server.controller;

import com.demo.lake.server.batch.model.BatchEvent;
import com.demo.lake.server.batch.model.BatchRun;
import com.demo.lake.server.batch.model.RunsView;
import com.demo.lake.server.batch.service.BatchJobService;
import com.demo.lake.server.batch.service.BatchWatcher;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
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
@Tag(name = "批作业编排", description = "触发 Spark 批作业、查看运行记录与日志。查看需 QUERY，触发需 BATCH 角色")
public class BatchController {

    private final BatchJobService jobService;
    private final BatchWatcher watcher;
    private final Scheduler blocking;

    public record RunDetail(
            @Schema(description = "运行记录（状态、时间、耗时、日志路径等）") BatchRun run,
            @Schema(description = "日志尾部若干行") List<String> tail) {
    }

    @Operation(summary = "触发批作业", description = """
            202 表示作业已排队；进度看 /api/batch/runs/{id}/events（SSE）或 /api/batch/runs/{id}。
            job 必须在白名单内；带 Idempotency-Key 头时重复提交返回同一条运行记录，不会重复执行。
            需要 BATCH 角色。
            """)
    @ApiResponses({
            @ApiResponse(responseCode = "202", description = "作业已接受并排队，返回体含 runId"),
            @ApiResponse(responseCode = "400", description = "作业名不在白名单或参数不合法")
    })
    @PostMapping("/{job}")
    public Mono<ResponseEntity<BatchRun>> submit(
            @Parameter(description = "作业名（白名单：init / ingest / transform / maintain / show 等）", example = "ingest")
            @PathVariable String job,
            @Parameter(description = "幂等键；重复提交同一键返回同一条运行记录")
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        // 建日志目录 + 写运行状态都是阻塞动作，扔到 blocking 调度器上
        return Mono.fromCallable(() -> jobService.submit(job, idempotencyKey))
                .subscribeOn(blocking)
                .map(run -> ResponseEntity.status(HttpStatus.ACCEPTED).body(run));
    }

    @Operation(summary = "运行记录列表", description = "本进程内存记录 ∪ ops.batch_runs 历史（按 id 合并，内存优先）")
    @GetMapping("/runs")
    public Mono<RunsView> runs(
            @Parameter(description = "最多返回多少条，默认 20") @RequestParam(defaultValue = "20") int limit) {
        return Mono.fromCallable(() -> jobService.list(limit)).subscribeOn(blocking);
    }

    @Operation(summary = "运行详情", description = "运行状态 + 日志尾部若干行")
    @GetMapping("/runs/{id}")
    public Mono<RunDetail> detail(
            @Parameter(description = "runId") @PathVariable String id,
            @Parameter(description = "日志尾部行数，默认 40") @RequestParam(defaultValue = "40") int tail) {
        // 日志尾部要读文件，同样别在事件循环上做
        return Mono.fromCallable(() -> new RunDetail(jobService.get(id), jobService.tail(id, tail)))
                .subscribeOn(blocking);
    }

    @Operation(summary = "作业监控流（SSE）", description = """
            事件：status（状态变化）/ log（日志增量）/ ping（心跳）/ end（终态，随后关闭）。
            一条长连接顶掉轮询；tail 是首帧回放的历史日志行数。
            """)
    @GetMapping(value = "/runs/{id}/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<BatchEvent>> events(
            @Parameter(description = "runId") @PathVariable String id,
            @Parameter(description = "首帧回放的历史日志行数，默认 80") @RequestParam(defaultValue = "80") int tail) {
        return watcher.watch(id, tail)
                .map(ev -> ServerSentEvent.builder(ev).event(ev.type()).build());
    }
}
