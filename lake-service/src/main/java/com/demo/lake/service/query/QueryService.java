package com.demo.lake.service.query;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.service.config.LakeProperties;
import com.facebook.presto.jdbc.PrestoResultSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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

/** Presto 查询执行：连接来自 Hikari 池，SQL 先过 {@link SqlGuard} */
@Service
public class QueryService {

    private static final Logger log = LoggerFactory.getLogger(QueryService.class);
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

    private final DataSource dataSource;
    private final LakeProperties props;
    private final PlatformConfig cfg;

    public QueryService(DataSource dataSource, LakeProperties props, PlatformConfig cfg) {
        this.dataSource = dataSource;
        this.props = props;
        this.cfg = cfg;
    }

    public QueryResult query(String sql, Integer maxRows) {
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
                log.info("query ok rows={} truncated={} {}ms sql={}", rows.size(), truncated, elapsedMs, abbreviate(safeSql));
                return new QueryResult(columns, rows, rows.size(), truncated, elapsedMs, stats(rs));
            }
        } catch (SQLException e) {
            throw translate(e);
        }
    }

    /** information_schema 列元数据表，走 Presto 自己的系统表，不额外维护表清单 */
    public List<Map<String, String>> listTables() {
        String sql = "select table_schema, table_name, table_type from "
                + catalog() + ".information_schema.tables where table_schema = '" + database() + "'"
                + " order by table_name";
        QueryResult rs = query(sql, null);
        List<Map<String, String>> out = new ArrayList<>(rs.rows().size());
        for (List<String> row : rs.rows()) {
            Map<String, String> m = new LinkedHashMap<>();
            m.put("schema", row.get(0));
            m.put("table", row.get(1));
            m.put("type", row.get(2));
            out.add(m);
        }
        return out;
    }

    public QueryResult sample(String table, int limit) {
        int n = Math.max(1, Math.min(limit, props.getQuery().getMaxRows()));
        String sql = "select * from " + catalog() + "." + database() + "." + identifier(table) + " limit " + n;
        return query(sql, n);
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
            return new PrestoUnavailableException(
                    "连不上 Presto，确认 coordinator 已启动（bin/infra.sh up）且 platform.presto.host/port 正确：" + msg, e);
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
            if (rs instanceof PrestoResultSet presto) {
                Object s = presto.getStats();
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
