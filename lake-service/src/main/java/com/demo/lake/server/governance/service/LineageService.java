package com.demo.lake.server.governance.service;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.lineage.ColumnEdge;
import com.demo.lake.common.lineage.LineageCatalog;
import com.demo.lake.server.governance.model.LineageView;
import com.demo.lake.server.governance.model.LineageView.ColumnEdgeView;
import com.demo.lake.server.governance.model.LineageView.RunView;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.service.QueryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 血缘查询：声明部分来自转换 SQL 的 @lineage 注释，运行部分来自 ops.lineage_runs。
 *
 * <p>从 gold 查时 upstream 给出 silver → bronze，runs 给出每次写入的 run_id 与 snapshot_id；
 * 表名走白名单，不把用户输入拼进 SQL。</p>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class LineageService {

    private static final String SELECT_RUNS = """
            SELECT run_id, job, snapshot_id, recorded_ms FROM %s WHERE table_name = ?
            ORDER BY recorded_ms DESC LIMIT %d
            """;
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final int MAX_LIMIT = 100;

    private final QueryService queries;
    private final PlatformConfig cfg;
    private final Scheduler blocking;

    public Mono<LineageView> forTable(String table, int limit) {
        if (!LineageCatalog.knownTable(table)) {
            return Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST, "未知业务表: " + table));
        }
        return Mono.fromCallable(() -> new LineageView(
                table,
                LineageCatalog.upstream(table),
                LineageCatalog.downstream(table),
                columns(table),
                runs(table, limit))).subscribeOn(blocking);
    }

    private static List<ColumnEdgeView> columns(String table) {
        List<ColumnEdgeView> out = new ArrayList<>();
        for (ColumnEdge edge : LineageCatalog.columnsInto(table)) {
            out.add(new ColumnEdgeView(edge.getSourceTable(), edge.getSourceColumn(),
                    edge.getTargetTable(), edge.getTargetColumn()));
        }
        return List.copyOf(out);
    }

    private List<RunView> runs(String table, int limit) {
        try {
            String sql = String.format(SELECT_RUNS, table(), Math.clamp(limit, 1, MAX_LIMIT));
            QueryResult result = queries.executeQuiet(sql, null, table);
            List<RunView> out = new ArrayList<>();
            for (List<String> row : result.rows()) {
                out.add(new RunView(text(row, 0), text(row, 1), text(row, 2), toLong(text(row, 3))));
            }
            return List.copyOf(out);
        } catch (RuntimeException e) {
            log.warn("读写入血缘失败 {}: {}", table, e.getMessage());
            return List.of();
        }
    }

    private String table() {
        String catalog = cfg.catalogName();
        if (catalog == null || !IDENTIFIER.matcher(catalog).matches()) {
            throw new IllegalStateException("非法 catalog 名: " + catalog);
        }
        return catalog + "." + LakeTables.OPS_SCHEMA + "." + LakeTables.LINEAGE_RUNS;
    }

    private static String text(List<String> row, int index) {
        return index < row.size() ? row.get(index) : null;
    }

    private static Long toLong(String value) {
        try {
            return value == null || value.isBlank() ? null : Long.valueOf(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
