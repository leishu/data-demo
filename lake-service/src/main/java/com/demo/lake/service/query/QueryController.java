package com.demo.lake.service.query;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.service.config.LakeProperties;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 即席查询接口：/api/query 为主，另附表清单、采样、配置回显 */
@RestController
@RequestMapping("/api")
public class QueryController {

    private final QueryService queryService;
    private final PlatformConfig cfg;
    private final LakeProperties props;

    public QueryController(QueryService queryService, PlatformConfig cfg, LakeProperties props) {
        this.queryService = queryService;
        this.cfg = cfg;
        this.props = props;
    }

    public record QueryRequest(String sql, Integer maxRows) {
    }

    @PostMapping("/query")
    public QueryResult query(@RequestBody QueryRequest request) {
        return queryService.query(request.sql(), request.maxRows());
    }

    @GetMapping("/tables")
    public List<Map<String, String>> tables() {
        return queryService.listTables();
    }

    @GetMapping("/tables/{table}/sample")
    public QueryResult sample(@PathVariable String table,
                              @RequestParam(defaultValue = "10") int limit) {
        return queryService.sample(table, limit);
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
        out.put("prestoUrl", cfg.prestoJdbcUrl());
        out.put("query", Map.of(
                "maxRows", props.getQuery().getMaxRows(),
                "timeoutSeconds", props.getQuery().getTimeoutSeconds(),
                "poolSize", props.getQuery().getPoolSize(),
                "allowWrites", props.getQuery().isAllowWrites()));
        out.put("batch", Map.of(
                "enabled", props.getBatch().isEnabled(),
                "commandTemplate", props.getBatch().getCommandTemplate(),
                "allowedJobs", props.getBatch().getAllowedJobs()));
        return out;
    }
}
