package com.demo.lake.server.controller;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.batch.model.JobView;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.exception.ErrorMapper;
import com.demo.lake.server.observability.AuditService;
import com.demo.lake.server.observability.AuditTextStore;
import com.demo.lake.server.observability.LakeMetrics;
import com.demo.lake.server.query.model.QueryEvent;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.model.TableInfo;
import com.demo.lake.server.query.service.QueryQuotaService;
import com.demo.lake.server.query.service.QueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.security.Principal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 即席查询接口：/api/query 为主，另附表清单、采样、配置回显；/api/query/stream 是 SSE 版本 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Tag(name = "即席查询", description = "Trino SQL 查询（同步 / SSE）、表清单与采样。需要 QUERY 角色")
public class QueryController {

    private final QueryService queryService;
    private final QueryQuotaService quotas;
    private final AuditService audit;
    private final AuditTextStore auditText;
    private final LakeMetrics metrics;
    private final PlatformConfig cfg;
    private final LakeProperties props;

    public record QueryRequest(
            @Schema(description = "SQL 文本；只读白名单：select / with / show / describe / explain，必须单条语句",
                    example = "select * from lake.default.orders limit 10") String sql,
            @Schema(description = "最多返回行数；不传用 lake.query.max-rows", example = "100") Integer maxRows) {
    }

    @Operation(summary = "同步执行 SQL", description = "返回列定义、行、耗时与 truncated 标记。受并发与每分钟配额约束")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "查询结果"),
            @ApiResponse(responseCode = "400", description = "SQL 不合法或未通过只读白名单"),
            @ApiResponse(responseCode = "429", description = "查询配额超限（并发数或每分钟次数）"),
            @ApiResponse(responseCode = "503", description = "Trino 暂不可用")
    })
    @PostMapping("/query")
    public Mono<QueryResult> query(@RequestBody QueryRequest request,
                                   @Parameter(hidden = true) Principal principal,
                                   @Parameter(hidden = true) ServerHttpRequest http) {
        // 配额只罩用户发起的查询：内部读运行历史走 QueryService，不占名额
        String actor = client(principal);
        String ip = clientIp(http);
        return Mono.usingWhen(quotas.acquire(actor),
                        permit -> queryService.query(request.sql(), request.maxRows()),
                        quotas::release)
                .doOnSuccess(result -> {
                    metrics.queryExecuted(true, result.elapsedMs(), result.rows().size());
                    auditQuery(actor, ip, request.sql(), "success", result.rows().size());
                })
                .doOnError(e -> {
                    metrics.queryExecuted(false, 0, 0);
                    auditQuery(actor, ip, request.sql(), "error", 0);
                });
    }

    /** 审计日志只留指纹；原文（脱敏+加密）按需写进独立保险箱 */
    private void auditQuery(String actor, String ip, String sql, String outcome, int rows) {
        String eventId = audit.record("query.execute", actor, Map.of(
                "sql_fp", AuditService.fingerprint(sql),
                "sql_len", String.valueOf(sql == null ? 0 : sql.length()),
                "outcome", outcome,
                "rows", String.valueOf(rows)));
        auditText.save(eventId, actor, ip, sql);
    }

    private static String clientIp(ServerHttpRequest http) {
        if (http == null || http.getRemoteAddress() == null || http.getRemoteAddress().getAddress() == null) {
            return null;
        }
        return http.getRemoteAddress().getAddress().getHostAddress();
    }

    /**
     * 同一条 SQL 的流式版本：meta → rows(多批) → done，失败则 error。
     *
     * <p>用 POST 而不是 GET（EventSource 只支持 GET）：SQL 放 query string 会被访问日志完整记下来，
     * 也容易被 URL 长度限制卡住；前端用 fetch + ReadableStream 解帧，代价只是几十行解析代码。</p>
     */
    @Operation(summary = "流式执行 SQL（SSE）", description = """
            同一条 SQL 的流式版本，事件依次是 meta → rows(多批) → done；失败发 error 事件。
            用 POST 是为了把 SQL 放在 body 而不是 URL（避免访问日志记录原文和长度限制），
            前端需 fetch + ReadableStream 解帧，EventSource 不适用。
            """)
    @PostMapping(value = "/query/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<QueryEvent>> queryStream(@RequestBody QueryRequest request,
                                                         @Parameter(hidden = true) Principal principal,
                                                         @Parameter(hidden = true) ServerHttpRequest http) {
        String actor = client(principal);
        String ip = clientIp(http);
        return Flux.usingWhen(quotas.acquire(actor),
                        permit -> queryService.streamQuery(request.sql(), request.maxRows()),
                        quotas::release)
                .map(ev -> ServerSentEvent.builder(ev).event(ev.type()).build())
                .onErrorResume(e -> {
                    // 流已经开始了就发不了状态码，只能自己翻成一条 error 事件
                    ErrorMapper.Mapped mapped = ErrorMapper.map(e);
                    return Flux.just(ServerSentEvent.<QueryEvent>builder(
                                    new QueryEvent.Error(mapped.status().value(), mapped.message(), mapped.hint()))
                            .event("error")
                            .build());
                })
                .doOnNext(sse -> {
                    if (sse.data() instanceof QueryEvent.Done done) {
                        metrics.queryExecuted(true, done.elapsedMs(), done.rowCount());
                        auditQuery(actor, ip, request.sql(), "success", done.rowCount());
                    } else if (sse.data() instanceof QueryEvent.Error) {
                        metrics.queryExecuted(false, 0, 0);
                        auditQuery(actor, ip, request.sql(), "error", 0);
                    }
                });
    }

    @Operation(summary = "表清单", description = "读 information_schema，返回当前库的表及字段（字段 comment 即 DDL 里的中文名）")
    @GetMapping("/tables")
    public Mono<List<TableInfo>> tables(@Parameter(hidden = true) Principal principal) {
        return Mono.usingWhen(quotas.acquire(client(principal)),
                permit -> queryService.listTables(),
                quotas::release);
    }

    @Operation(summary = "表采样", description = "按 limit 采样一张表，返回结构与 /api/query 相同")
    @GetMapping("/tables/{table}/sample")
    public Mono<QueryResult> sample(
            @Parameter(description = "表名", example = "orders") @PathVariable String table,
            @Parameter(description = "采样行数，默认 10") @RequestParam(defaultValue = "10") int limit,
            @Parameter(hidden = true) Principal principal) {
        return Mono.usingWhen(quotas.acquire(client(principal)),
                permit -> queryService.sample(table, limit),
                quotas::release);
    }

    /** 配额按认证账号分桶；没拿到 Principal（鉴权关闭时）退回匿名桶 */
    private static String client(Principal principal) {
        return principal == null || principal.getName() == null ? "anonymous" : principal.getName();
    }

    /** 有效配置回显：只给非敏感项，S3 密钥用 PlatformConfig.describe() 打码后的结果 */
    @Operation(summary = "生效配置回显", description = "只回显非敏感项；S3 密钥等由 PlatformConfig.describe() 打码")
    @GetMapping("/config")
    public Map<String, Object> config() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("platform", cfg.describe());
        out.put("catalog", cfg.catalogName());
        out.put("database", cfg.database());
        out.put("warehouse", cfg.warehouse());
        out.put("writeMode", cfg.writeMode());
        out.put("trinoUrl", cfg.trinoJdbcUrl());
        out.put("query", Map.of(
                "maxRows", props.getQuery().getMaxRows(),
                "timeoutSeconds", props.getQuery().getTimeoutSeconds(),
                "poolSize", props.getQuery().getPoolSize(),
                "allowWrites", props.getQuery().isAllowWrites()));
        Map<String, Object> batch = new LinkedHashMap<>();
        batch.put("enabled", props.getBatch().isEnabled());
        batch.put("commandTemplate", props.getBatch().getCommandTemplate());
        batch.put("allowedJobs", props.getBatch().getAllowedJobs());
        // 页面卡片：白名单 ∩ LakeJobs.METAS。标题/描述只在 common 维护一份，前端纯渲染
        batch.put("jobs", JobView.of(LakeJobs.visible(props.getBatch().getAllowedJobs())));
        out.put("batch", batch);
        return out;
    }
}
