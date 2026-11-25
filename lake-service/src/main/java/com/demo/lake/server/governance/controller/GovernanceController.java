package com.demo.lake.server.governance.controller;

import com.demo.lake.server.governance.model.DqResultView;
import com.demo.lake.server.governance.model.GovernanceAsset;
import com.demo.lake.server.governance.model.LineageView;
import com.demo.lake.server.governance.model.RetentionAuditView;
import com.demo.lake.server.governance.service.DqResultService;
import com.demo.lake.server.governance.service.GovernanceService;
import com.demo.lake.server.governance.service.LineageService;
import com.demo.lake.server.governance.service.RetentionAuditService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;

/** 资产目录接口：只读，需要 QUERY 角色；表清单读不到时照常透出 503 */
@RestController
@RequestMapping("/api/governance")
@RequiredArgsConstructor
@Tag(name = "资产目录", description = "表级 owner / 业务域 / 分层 / 敏感级别 / 保留期 / 新鲜度 SLA；需要 QUERY 角色")
public class GovernanceController {

    private final GovernanceService service;
    private final DqResultService dq;
    private final LineageService lineage;
    private final RetentionAuditService retention;

    @Operation(summary = "治理资产清单",
            description = "GovernanceCatalog ∪ information_schema，并合并 Iceberg 文件数/快照数/最近更新时间")
    @GetMapping("/assets")
    public Mono<List<GovernanceAsset>> assets() {
        return service.assets(System.currentTimeMillis());
    }

    @Operation(summary = "质量检查结果",
            description = "ops.dq_results 最近 N 条，含 observed / threshold / 证据与所属 run_id；表不存在时返回空列表")
    @GetMapping("/dq")
    public Mono<List<DqResultView>> dq(
            @Parameter(description = "返回条数，默认 50，最大 100") @RequestParam(defaultValue = "50") int limit) {
        return dq.recent(limit);
    }

    @Operation(summary = "表血缘",
            description = "上游/下游与列级声明（来自转换 SQL 注释）+ 该表最近写入的 run_id / snapshot_id")
    @GetMapping("/lineage")
    public Mono<LineageView> lineage(
            @Parameter(description = "业务表名：bronze_orders / silver_orders / gold_daily_revenue") @RequestParam String table,
            @Parameter(description = "返回条数，默认 50，最大 100") @RequestParam(defaultValue = "50") int limit) {
        return lineage.forTable(table, limit);
    }

    @Operation(summary = "保留审计事件",
            description = "ops.retention_audit 最近 N 条，含 dry_run 标记与命中分区/行数")
    @GetMapping("/retention")
    public Mono<List<RetentionAuditView>> retention(
            @Parameter(description = "返回条数，默认 50，最大 100") @RequestParam(defaultValue = "50") int limit) {
        return retention.recent(limit);
    }
}
