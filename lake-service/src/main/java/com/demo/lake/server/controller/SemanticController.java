package com.demo.lake.server.controller;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.semantic.MetricDefinition;
import com.demo.lake.common.semantic.SemanticCatalog;
import com.demo.lake.common.semantic.SemanticFilter;
import com.demo.lake.common.semantic.SemanticSqlRenderer;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.service.AuditService;
import com.demo.lake.server.observability.store.AuditTextStore;
import com.demo.lake.server.observability.service.LakeMetrics;
import com.demo.lake.server.query.service.InvalidSemanticQueryException;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.service.QueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.server.reactive.ServerHttpRequest;
import reactor.core.publisher.Mono;

import java.security.Principal;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 语义层接口：调用方选指标/维度/过滤，服务端按统一口径生成 SQL 并执行。
 *
 * <p>同一个定义可以渲染成 Trino 或 Spark 方言（引号风格不同、口径一致），
 * 但执行永远走 Trino；render 接口只给需要多引擎下推的调用方拿 SQL。</p>
 */
@RestController
@RequestMapping("/api/semantics")
@RequiredArgsConstructor
@Tag(name = "语义层", description = "调用方选指标 / 维度 / 过滤，服务端按统一口径生成 SQL；需要 QUERY 角色")
public class SemanticController {

    private final PlatformConfig platformConfig;
    private final LakeProperties properties;
    private final QueryService queryService;
    private final AuditService auditService;
    private final AuditTextStore auditTextStore;
    private final LakeMetrics metrics;

    public record FilterItem(
            @Schema(description = "过滤字段，必须在指标允许的字段白名单内", example = "status") String field,
            @Schema(description = "过滤操作符（由渲染器白名单校验）", example = "eq") String op,
            @Schema(description = "过滤值", example = "PAID") String value) {
    }

    public record SemanticRequest(
            @Schema(description = "指标名，见 GET /api/semantics", example = "gmv") String metric,
            @Schema(description = "维度列表，必须在指标允许的维度白名单内", example = "[\"dt\"]") List<String> dimensions,
            @Schema(description = "过滤条件列表") List<FilterItem> filters,
            @Schema(description = "返回行数上限；不传用 lake.query.max-rows", example = "100") Integer limit,
            @Schema(description = "仅 render 生效：trino（默认）或 spark；query 固定用 trino", example = "trino") String engine) {
    }

    @Operation(summary = "指标目录", description = "口径表达式、来源表与允许的维度")
    @GetMapping
    public List<MetricDefinition> definitions() {
        return SemanticCatalog.METRICS;
    }

    @Operation(summary = "单个指标定义", description = "口径表达式、来源表与允许的维度")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "指标定义"),
            @ApiResponse(responseCode = "400", description = "没有这个指标")
    })
    @GetMapping("/{metric}")
    public MetricDefinition definition(
            @Parameter(description = "指标名", example = "gmv") @PathVariable String metric) {
        MetricDefinition found = SemanticCatalog.get(metric);
        if (found == null) {
            throw new InvalidSemanticQueryException("没有这个指标: " + metric);
        }
        return found;
    }

    @Operation(summary = "生成 SQL（不执行）", description = "engine=trino（默认）或 spark，可对比两种方言；返回 {engine, sql}")
    @PostMapping("/render")
    public Map<String, Object> render(@RequestBody SemanticRequest request) {
        SemanticSqlRenderer.Dialect dialect = dialect(request.engine());
        return Map.of("engine", dialect.name().toLowerCase(Locale.ROOT), "sql", sql(request, dialect));
    }

    @Operation(summary = "执行语义查询", description = "固定用 Trino 方言；同样走配额与审计，返回结构与 /api/query 相同")
    @PostMapping("/query")
    public Mono<QueryResult> query(@RequestBody SemanticRequest request,
                                   @Parameter(hidden = true) Principal principal,
                                   @Parameter(hidden = true) ServerHttpRequest http) {
        String sql = sql(request, SemanticSqlRenderer.Dialect.TRINO);
        String actor = principal == null || principal.getName() == null ? "anonymous" : principal.getName();
        String ip = http == null || http.getRemoteAddress() == null
                ? null : http.getRemoteAddress().getAddress().getHostAddress();
        return queryService.queryWithQuota(actor, sql, request.limit())
                .doOnSuccess(result -> {
                    metrics.queryExecuted(true, result.elapsedMs(), result.rows().size());
                    String eventId = auditService.record("semantic.query", actor, Map.of(
                            "metric", String.valueOf(request.metric()),
                            "sql_fp", AuditService.fingerprint(sql),
                            "rows", String.valueOf(result.rows().size())));
                    auditTextStore.save(eventId, actor, ip, sql);
                })
                .doOnError(e -> {
                    metrics.queryExecuted(false, 0, 0);
                    auditService.record("semantic.query", actor, Map.of(
                            "metric", String.valueOf(request.metric()), "outcome", "error"));
                });
    }

    private static SemanticSqlRenderer.Dialect dialect(String engine) {
        if (engine == null || engine.isBlank() || "trino".equalsIgnoreCase(engine)) {
            return SemanticSqlRenderer.Dialect.TRINO;
        }
        if ("spark".equalsIgnoreCase(engine)) {
            return SemanticSqlRenderer.Dialect.SPARK;
        }
        throw new InvalidSemanticQueryException("不支持的引擎: " + engine + "（可选 trino / spark）");
    }

    private String sql(SemanticRequest request, SemanticSqlRenderer.Dialect dialect) {
        if (request == null || request.metric() == null) {
            throw new InvalidSemanticQueryException("metric 不能为空");
        }
        MetricDefinition metric = SemanticCatalog.get(request.metric());
        if (metric == null) {
            throw new InvalidSemanticQueryException("没有这个指标: " + request.metric());
        }
        List<SemanticFilter> filters = request.filters() == null ? List.of()
                : request.filters().stream()
                        .map(f -> new SemanticFilter(f.field(), f.op(), f.value()))
                        .toList();
        int limit = request.limit() == null || request.limit() <= 0
                ? properties.getQuery().getMaxRows() : request.limit();
        String table = platformConfig.catalogName() + "." + platformConfig.database() + "." + metric.getSource();
        try {
            return SemanticSqlRenderer.render(metric, table, request.dimensions(), filters, limit, dialect);
        } catch (IllegalArgumentException e) {
            // 渲染器的白名单校验失败是「调用方请求不合法」，应当是 400 而不是 500
            throw new InvalidSemanticQueryException(e.getMessage());
        }
    }
}
