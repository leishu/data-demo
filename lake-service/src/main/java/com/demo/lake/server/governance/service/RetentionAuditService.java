package com.demo.lake.server.governance.service;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.governance.model.RetentionAuditView;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.service.QueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.regex.Pattern;

/** 读 ops.retention_audit：保留作业执行前后的审计事件，dry-run 也留痕 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RetentionAuditService {

    private static final String SELECT = """
            SELECT run_id, event, table_name, dry_run, partitions, row_count, audited_ms, detail
            FROM %s ORDER BY audited_ms DESC LIMIT %d
            """;
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final int MAX_LIMIT = 100;

    private final QueryService queries;
    private final PlatformConfig cfg;
    private final Scheduler blocking;

    public Mono<List<RetentionAuditView>> recent(int limit) {
        return Mono.fromCallable(() -> load(Math.clamp(limit, 1, MAX_LIMIT))).subscribeOn(blocking);
    }

    private List<RetentionAuditView> load(int n) {
        try {
            QueryResult result = queries.executeQuiet(String.format(SELECT, table(), n), null);
            List<RetentionAuditView> out = new java.util.ArrayList<>();
            for (List<String> row : result.rows()) {
                out.add(new RetentionAuditView(
                        text(row, 0), text(row, 1), text(row, 2), Boolean.parseBoolean(text(row, 3)),
                        toInt(text(row, 4)), toLong(text(row, 5)), toLong(text(row, 6)), text(row, 7)));
            }
            return List.copyOf(out);
        } catch (RuntimeException e) {
            log.warn("读保留审计事件失败: {}", e.getMessage());
            return List.of();
        }
    }

    private String table() {
        String catalog = cfg.catalogName();
        if (catalog == null || !IDENTIFIER.matcher(catalog).matches()) {
            throw new IllegalStateException("非法 catalog 名: " + catalog);
        }
        return catalog + "." + LakeTables.OPS_SCHEMA + "." + LakeTables.RETENTION_AUDIT;
    }

    private static String text(List<String> row, int index) {
        return index < row.size() ? row.get(index) : null;
    }

    private static int toInt(String value) {
        try {
            return value == null || value.isBlank() ? 0 : Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private static long toLong(String value) {
        try {
            return value == null || value.isBlank() ? 0 : Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
