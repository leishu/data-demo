package com.demo.lake.server.governance.service;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.governance.model.DqResultView;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.service.QueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.List;
import java.util.regex.Pattern;

/**
 * 读湖表 {@code ops.dq_results}（quality 作业写入的失败/通过证据）。
 *
 * <p>和 {@code BatchHistoryService} 一样是旁路数据：表还没建或 Trino 不可用时返回空列表，
 * 不让治理页因为质量问题打不开。阻塞 JDBC 走 blocking 调度器。</p>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DqResultService {

    private static final String SELECT = """
            SELECT run_id, snapshot_id, "check", severity, observed, threshold, detail, checked_ms
            FROM %s ORDER BY checked_ms DESC LIMIT %d
            """;
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final int MAX_LIMIT = 100;

    private final QueryService queryService;
    private final PlatformConfig platformConfig;
    private final Scheduler blocking;

    public Mono<List<DqResultView>> recent(int limit) {
        return Mono.fromCallable(() -> {
            try {
                return loadStrict(Math.clamp(limit, 1, MAX_LIMIT));
            } catch (RuntimeException e) {
                // 治理页是旁路：表还没建或 Trino 不可用时返回空列表，别让页面打不开
                log.warn("读质量检查结果失败: {}", e.getMessage());
                return List.<DqResultView>of();
            }
        }).subscribeOn(blocking);
    }

    /**
     * 严格版本：读失败要抛给调用方，让指标采集能区分「全部通过」和「根本没读到」。
     * GovernanceMetrics 用它避免把瞬时故障当成告警恢复。
     */
    public Mono<List<DqResultView>> recentStrict(int limit) {
        return Mono.fromCallable(() -> loadStrict(Math.clamp(limit, 1, MAX_LIMIT))).subscribeOn(blocking);
    }

    private List<DqResultView> loadStrict(int cappedLimit) {
        QueryResult result = queryService.executeQuiet(String.format(SELECT, table(), cappedLimit), null);
        return result.rows().stream().map(DqResultService::toView).toList();
    }

    private String table() {
        String catalog = platformConfig.catalogName();
        if (catalog == null || !IDENTIFIER.matcher(catalog).matches()) {
            throw new IllegalStateException("非法 catalog 名: " + catalog);
        }
        return catalog + "." + LakeTables.OPS_SCHEMA + "." + LakeTables.DQ_RESULTS;
    }

    private static DqResultView toView(List<String> row) {
        Double observed = toDouble(text(row, 4));
        Double threshold = toDouble(text(row, 5));
        boolean passed = observed != null && threshold != null && observed <= threshold;
        return new DqResultView(
                text(row, 0), text(row, 1), text(row, 2), text(row, 3),
                observed, threshold, text(row, 6), toLong(text(row, 7)), passed);
    }

    private static String text(List<String> row, int index) {
        return index < row.size() ? row.get(index) : null;
    }

    private static Double toDouble(String value) {
        try {
            return value == null || value.isBlank() ? null : Double.valueOf(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long toLong(String value) {
        try {
            return value == null || value.isBlank() ? null : Long.valueOf(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
