package com.demo.lake.server.query.service;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.query.*;
import com.demo.lake.server.query.model.Column;
import com.demo.lake.server.query.model.QueryEvent;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.model.TableInfo;
import io.trino.jdbc.TrinoResultSet;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Trino 查询执行：连接来自 Hikari 池，SQL 先过 {@link SqlGuard}。
 *
 * <p>JDBC 是阻塞 API，所以对外返回 Mono/Flux，并 {@code subscribeOn(blocking)} 到虚拟线程上执行。
 * Hikari 与 ResultSet 并没有变成响应式：那需要 r2dbc 驱动，Trino 没有，硬包一层只是把阻塞藏起来。</p>
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class QueryService {

    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    /** SSE 每批发多少行：太小帧数爆炸，太大首屏等得久 */
    private static final int STREAM_BATCH_ROWS = 200;

    private final DataSource dataSource;
    private final LakeProperties props;
    private final PlatformConfig cfg;
    private final Scheduler blocking;

    /** 一次性返回完整结果（JSON 接口，也是控制台之外调用方最省事的形态） */
    public Mono<QueryResult> query(String sql, Integer maxRows) {
        return Mono.fromCallable(() -> execute(sql, maxRows)).subscribeOn(blocking);
    }

    /**
     * 周期性轮询（如运行历史列表）走这条：SQL 固定、几秒一次，INFO 会刷满日志，降到 debug 保留可查性。
     */
    public QueryResult executeQuiet(String sql, Integer maxRows) {
        return execute(sql, maxRows, true);
    }

    /**
     * 同步执行的实现，只允许在 blocking 调度器线程里调用。
     *
     * <p>public 是为了给 {@code BatchHistoryService} 这类内部调用方复用：错误翻译与
     * {@code SqlGuard} 都在这里，重写一遍 JDBC 只会把同一个坑再踩一次。</p>
     */
    public QueryResult execute(String sql, Integer maxRows) {
        return execute(sql, maxRows, false);
    }

    private QueryResult execute(String sql, Integer maxRows, boolean quiet) {
        String safeSql = SqlGuard.requireReadOnly(sql, props.getQuery().isAllowWrites());
        int cap = props.getQuery().getMaxRows();
        int limit = (maxRows == null || maxRows <= 0) ? cap : Math.min(maxRows, cap);

        long start = System.nanoTime();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(props.getQuery().getTimeoutSeconds());
            try (ResultSet rs = stmt.executeQuery(safeSql)) {
                ResultSetMetaData md = rs.getMetaData();
                int columnCount = md.getColumnCount();
                List<Column> columns = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    columns.add(new Column(md.getColumnLabel(i), md.getColumnTypeName(i)));
                }

                List<List<String>> rows = new ArrayList<>();
                boolean truncated = false;
                while (rs.next()) {
                    if (rows.size() >= limit) {
                        truncated = true;
                        break;
                    }
                    List<String> row = new ArrayList<>(columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        row.add(format(rs.getObject(i)));
                    }
                    rows.add(row);
                }

                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                if (quiet) {
                    log.debug("query ok rows={} truncated={} {}ms sql={}", rows.size(), truncated, elapsedMs, abbreviate(safeSql));
                } else {
                    log.info("query ok rows={} truncated={} {}ms sql={}", rows.size(), truncated, elapsedMs, abbreviate(safeSql));
                }
                return new QueryResult(columns, rows, rows.size(), truncated, elapsedMs, stats(rs));
            }
        } catch (SQLException e) {
            throw translate(e);
        }
    }

    /**
     * 流式查询：先发列定义，再按 {@value #STREAM_BATCH_ROWS} 行一批发数据，最后发统计。
     *
     * <p>用 {@code Flux.create} 而不是 {@code generate}：这里是一条「边读 ResultSet 边发」的循环，
     * 天然有多个元素；行数被 maxRows 卡住，所以即使下游慢、sink 缓冲也是有界的。</p>
     */
    public Flux<QueryEvent> streamQuery(String sql, Integer maxRows) {
        return Flux.<QueryEvent>create(sink -> {
            long start = System.nanoTime();
            String safeSql;
            int limit;
            try {
                safeSql = SqlGuard.requireReadOnly(sql, props.getQuery().isAllowWrites());
                int cap = props.getQuery().getMaxRows();
                limit = (maxRows == null || maxRows <= 0) ? cap : Math.min(maxRows, cap);
            } catch (RuntimeException e) {
                sink.error(e);
                return;
            }
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(props.getQuery().getTimeoutSeconds());
                try (ResultSet rs = stmt.executeQuery(safeSql)) {
                    ResultSetMetaData md = rs.getMetaData();
                    int columnCount = md.getColumnCount();
                    List<Column> columns = new ArrayList<>(columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        columns.add(new Column(md.getColumnLabel(i), md.getColumnTypeName(i)));
                    }
                    sink.next(new QueryEvent.Meta(columns));

                    List<List<String>> batch = new ArrayList<>(STREAM_BATCH_ROWS);
                    int count = 0;
                    boolean truncated = false;
                    while (rs.next()) {
                        if (sink.isCancelled()) {
                            // 浏览器关页 / 前端 abort：别让 Trino 在那边继续算
                            stmt.cancel();
                            return;
                        }
                        if (count >= limit) {
                            truncated = true;
                            break;
                        }
                        List<String> row = new ArrayList<>(columnCount);
                        for (int i = 1; i <= columnCount; i++) {
                            row.add(format(rs.getObject(i)));
                        }
                        batch.add(row);
                        count++;
                        if (batch.size() >= STREAM_BATCH_ROWS) {
                            // copy 一份再清空，否则 sink 里拿到的就是同一个正在被清空的 list
                            sink.next(new QueryEvent.Rows(List.copyOf(batch)));
                            batch.clear();
                        }
                    }
                    if (!batch.isEmpty()) {
                        sink.next(new QueryEvent.Rows(List.copyOf(batch)));
                    }
                    long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                    sink.next(new QueryEvent.Done(count, truncated, elapsedMs, stats(rs)));
                    log.info("stream ok rows={} truncated={} {}ms sql={}", count, truncated, elapsedMs, abbreviate(safeSql));
                    sink.complete();
                }
            } catch (SQLException e) {
                sink.error(translate(e));
            } catch (RuntimeException e) {
                sink.error(e);
            }
        }, FluxSink.OverflowStrategy.BUFFER).subscribeOn(blocking);
    }

    /**
     * 表清单 + 字段清单，全部走 Trino 自己的 information_schema，不额外维护一份表结构。
     *
     * <p>一次 left join 把表和列一起捞回来：建表 DDL 里的 {@code COMMENT '订单号'} 就是
     * {@code information_schema.columns.comment}，页面直接拿它当字段中文名。</p>
     */
    public Mono<List<TableInfo>> listTables() {
        return Mono.fromCallable(() -> {
            String sql = "select t.table_name, t.table_type, c.column_name, c.data_type, c.comment"
                    + " from " + catalog() + ".information_schema.tables t"
                    + " left join " + catalog() + ".information_schema.columns c"
                    + " on c.table_schema = t.table_schema and c.table_name = t.table_name"
                    + " where t.table_schema = '" + database() + "'"
                    + " order by t.table_name, c.ordinal_position";
            QueryResult rs = execute(sql, null);
            // 表名 → 表信息；left join 下没有列的表只出现一行，column_name 为 null
            Map<String, TableInfo> byTable = new LinkedHashMap<>();
            for (List<String> row : rs.rows()) {
                TableInfo info = byTable.computeIfAbsent(row.get(0),
                        name -> new TableInfo(database(), name, row.get(1), new ArrayList<>()));
                if (row.get(2) != null) {
                    info.columns().add(new Column(row.get(2), row.get(3), row.get(4)));
                }
            }
            return List.copyOf(byTable.values());
        }).subscribeOn(blocking);
    }

    public Mono<QueryResult> sample(String table, int limit) {
        return Mono.fromCallable(() -> {
            int n = Math.max(1, Math.min(limit, props.getQuery().getMaxRows()));
            String sql = "select * from " + catalog() + "." + database() + "." + identifier(table) + " limit " + n;
            return execute(sql, n);
        }).subscribeOn(blocking);
    }

    private String catalog() {
        return identifier(cfg.catalogName());
    }

    private String database() {
        return identifier(cfg.database());
    }

    private static String identifier(String raw) {
        if (raw == null || !IDENTIFIER.matcher(raw).matches()) {
            throw new InvalidQueryException("非法标识符: " + raw);
        }
        return raw;
    }

    private static RuntimeException translate(SQLException e) {
        String msg = e.getMessage() == null ? e.toString() : e.getMessage();
        if (isConnectionIssue(e)) {
            return new TrinoUnavailableException(
                    "连不上 Trino，确认 coordinator 已启动（bin/infra.sh up trino）且 platform.trino.host/port 正确：" + msg, e);
        }
        return new QueryFailedException(msg, e.getSQLState(), e.getErrorCode(), e);
    }

    /**
     * 连接类错误的判定：SQLState 08xxx 是标准做法，但 Hikari 池拿不到连接时抛的
     * SQLTransientConnectionException 没有 SQLState，只能按类型 + 根因判断。
     */
    private static boolean isConnectionIssue(SQLException e) {
        if (e.getSQLState() != null && e.getSQLState().startsWith("08")) {
            return true;
        }
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof SQLTransientConnectionException || t instanceof SQLNonTransientConnectionException) {
                return true;
            }
            if (t instanceof java.net.ConnectException
                    || t instanceof java.net.UnknownHostException
                    || t instanceof java.net.SocketTimeoutException) {
                return true;
            }
            if (t == t.getCause()) {
                break;
            }
        }
        return false;
    }

    /** QueryStats 的字段随驱动版本变，只取 toString，避免绑死内部 API */
    private static String stats(ResultSet rs) {
        try {
            if (rs instanceof TrinoResultSet trinoResult) {
                Object s = trinoResult.getStats();
                return s == null ? null : s.toString();
            }
        } catch (RuntimeException ignore) {
            // 统计信息拿不到不影响结果
        }
        return null;
    }

    private static String format(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal decimal) {
            return decimal.toPlainString();
        }
        if (value instanceof Timestamp || value instanceof java.sql.Date || value instanceof java.sql.Time) {
            return value.toString();
        }
        if (value instanceof byte[] bytes) {
            return "<binary " + bytes.length + "B>";
        }
        return String.valueOf(value);
    }

    private static String abbreviate(String sql) {
        String single = sql.replaceAll("\\s+", " ");
        return single.length() <= 160 ? single : single.substring(0, 157) + "...";
    }
}
