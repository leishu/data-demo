package com.demo.lake.server.controller;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.batch.model.JobView;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.exception.ErrorMapper;
import com.demo.lake.server.query.model.QueryEvent;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.model.TableInfo;
import com.demo.lake.server.query.service.QueryQuotaService;
import com.demo.lake.server.query.service.QueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
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
public class QueryController {

    private final QueryService queryService;
    private final QueryQuotaService quotas;
    private final PlatformConfig cfg;
    private final LakeProperties props;

    public record QueryRequest(String sql, Integer maxRows) {
    }

    @PostMapping("/query")
    public Mono<QueryResult> query(@RequestBody QueryRequest request, Principal principal) {
        // 配额只罩用户发起的查询：内部读运行历史走 QueryService，不占名额
        return Mono.usingWhen(quotas.acquire(client(principal)),
                permit -> queryService.query(request.sql(), request.maxRows()),
                quotas::release);
    }

    /**
     * 同一条 SQL 的流式版本：meta → rows(多批) → done，失败则 error。
     *
     * <p>用 POST 而不是 GET（EventSource 只支持 GET）：SQL 放 query string 会被访问日志完整记下来，
     * 也容易被 URL 长度限制卡住；前端用 fetch + ReadableStream 解帧，代价只是几十行解析代码。</p>
     */
    @PostMapping(value = "/query/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<QueryEvent>> queryStream(@RequestBody QueryRequest request, Principal principal) {
        return Flux.usingWhen(quotas.acquire(client(principal)),
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
                });
    }

    @GetMapping("/tables")
    public Mono<List<TableInfo>> tables(Principal principal) {
        return Mono.usingWhen(quotas.acquire(client(principal)),
                permit -> queryService.listTables(),
                quotas::release);
    }

    @GetMapping("/tables/{table}/sample")
    public Mono<QueryResult> sample(@PathVariable String table,
                                    @RequestParam(defaultValue = "10") int limit,
                                    Principal principal) {
        return Mono.usingWhen(quotas.acquire(client(principal)),
                permit -> queryService.sample(table, limit),
                quotas::release);
    }

    /** 配额按认证账号分桶；没拿到 Principal（鉴权关闭时）退回匿名桶 */
    private static String client(Principal principal) {
        return principal == null || principal.getName() == null ? "anonymous" : principal.getName();
    }

    /** 有效配置回显：只给非敏感项，S3 密钥用 PlatformConfig.describe() 打码后的结果 */
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
