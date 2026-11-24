package com.demo.lake.server.governance.controller;

import com.demo.lake.server.governance.model.GovernanceAsset;
import com.demo.lake.server.governance.service.GovernanceService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
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

    @Operation(summary = "治理资产清单",
            description = "GovernanceCatalog ∪ information_schema，并合并 Iceberg 文件数/快照数/最近更新时间")
    @GetMapping("/assets")
    public Mono<List<GovernanceAsset>> assets() {
        return service.assets(System.currentTimeMillis());
    }
}
