package com.demo.lake.server.controller;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.semantic.MetricDefinition;
import com.demo.lake.common.semantic.SemanticCatalog;
import com.demo.lake.common.semantic.SemanticFilter;
import com.demo.lake.common.semantic.SemanticSqlRenderer;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.observability.AuditService;
import com.demo.lake.server.observability.AuditTextStore;
import com.demo.lake.server.observability.LakeMetrics;
import com.demo.lake.server.query.InvalidSemanticQueryException;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.service.QueryQuotaService;
import com.demo.lake.server.query.service.QueryService;
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
public class SemanticController {

    private final PlatformConfig cfg;
    private final LakeProperties props;
    private final QueryService queries;
    private final QueryQuotaService quotas;
    private final AuditService audit;
    private final AuditTextStore auditText;
    private final LakeMetrics metrics;

    public record Filter(String field, String op, String value) {
    }

    public record SemanticRequest(String metric, List<String> dimensions, List<Filter> filters,
                                  Integer limit, String engine) {
    }

    @GetMapping
    public List<MetricDefinition> definitions() {
        return SemanticCatalog.METRICS;
    }

    @GetMapping("/{metric}")
    public MetricDefinition definition(@PathVariable String metric) {
        MetricDefinition found = SemanticCatalog.get(metric);
        if (found == null) {
            throw new InvalidSemanticQueryException("没有这个指标: " + metric);
        }
        return found;
    }

    /** 只看 SQL，不执行：engine=trino（默认）| spark */
    @PostMapping("/render")
    public Map<String, Object> render(@RequestBody SemanticRequest request) {
        SemanticSqlRenderer.Dialect dialect = dialect(request.engine());
        return Map.of("engine", dialect.name().toLowerCase(Locale.ROOT), "sql", sql(request, dialect));
    }

    /** 执行：固定用 Trino 方言；查询同样走配额与审计 */
    @PostMapping("/query")
    public Mono<QueryResult> query(@RequestBody SemanticRequest request, Principal principal,
                                   ServerHttpRequest http) {
        String sql = sql(request, SemanticSqlRenderer.Dialect.TRINO);
        String actor = principal == null || principal.getName() == null ? "anonymous" : principal.getName();
        String ip = http == null || http.getRemoteAddress() == null
                ? null : http.getRemoteAddress().getAddress().getHostAddress();
        return Mono.usingWhen(quotas.acquire(actor), permit -> queries.query(sql, request.limit()), quotas::release)
                .doOnSuccess(result -> {
                    metrics.queryExecuted(true, result.elapsedMs(), result.rows().size());
                    String eventId = audit.record("semantic.query", actor, Map.of(
                            "metric", String.valueOf(request.metric()),
                            "sql_fp", AuditService.fingerprint(sql),
                            "rows", String.valueOf(result.rows().size())));
                    auditText.save(eventId, actor, ip, sql);
                })
                .doOnError(e -> {
                    metrics.queryExecuted(false, 0, 0);
                    audit.record("semantic.query", actor, Map.of(
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
                ? props.getQuery().getMaxRows() : request.limit();
        String table = cfg.catalogName() + "." + cfg.database() + "." + metric.getSource();
        try {
            return SemanticSqlRenderer.render(metric, table, request.dimensions(), filters, limit, dialect);
        } catch (IllegalArgumentException e) {
            // 渲染器的白名单校验失败是「调用方请求不合法」，应当是 400 而不是 500
            throw new InvalidSemanticQueryException(e.getMessage());
        }
    }
}
