package com.demo.lake.server.support;

import com.demo.lake.common.sql.SqlScript;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * 状态库（H2 / Postgres 双兼容）的 DDL 装载器。
 *
 * <p>SQL 文本放在 classpath 的 {@code sql/state/} 下，由各 Store 的 {@code @PostConstruct}
 * 按需取用。本类只负责读文件、按分号切句；「初始化失败是否阻止启动」仍由各 Store 自己决定
 * （审计保险箱 fail closed，状态库/日志表降级不阻止启动）。</p>
 *
 * <p>索引单独成文件，因为老版本 H2 不支持 {@code CREATE INDEX IF NOT EXISTS}，
 * 调用方需要能单独 try/catch 掉索引失败而保留建表成功。</p>
 */
public final class StateDbDdl {

    private static final String SCHEMA_BATCH = "sql/state/00_schema_batch.sql";
    private static final String SCHEMA_AUDIT = "sql/state/01_schema_audit.sql";
    private static final String SERVER_LOCKS = "sql/state/10_server_locks.sql";
    private static final String QUERY_LEASES = "sql/state/20_query_leases.sql";
    private static final String QUERY_EVENTS = "sql/state/21_query_events.sql";
    private static final String QUERY_INDEXES = "sql/state/22_query_indexes.sql";
    private static final String BATCH_RUNS = "sql/state/30_batch_runs.sql";
    private static final String BATCH_RUNS_INDEX = "sql/state/31_batch_runs_index.sql";
    private static final String BATCH_RUNS_ADD_HANDLE = "sql/state/32_batch_runs_add_handle.sql";
    private static final String IDEMPOTENCY = "sql/state/33_idempotency.sql";
    private static final String BATCH_LOG_LINES = "sql/state/40_batch_log_lines.sql";
    private static final String QUERY_TEXT = "sql/state/50_query_text.sql";

    private static final List<String> ALL = Collections.unmodifiableList(Arrays.asList(
            SCHEMA_BATCH, SCHEMA_AUDIT, SERVER_LOCKS,
            QUERY_LEASES, QUERY_EVENTS, QUERY_INDEXES,
            BATCH_RUNS, BATCH_RUNS_INDEX, BATCH_RUNS_ADD_HANDLE, IDEMPOTENCY,
            BATCH_LOG_LINES, QUERY_TEXT));

    private StateDbDdl() {
    }

    /** 全部模板，供单测遍历守卫。 */
    public static List<String> all() {
        return ALL;
    }

    public static String schemaBatch() {
        return one(SCHEMA_BATCH);
    }

    public static String schemaAudit() {
        return one(SCHEMA_AUDIT);
    }

    public static String serverLocks() {
        return one(SERVER_LOCKS);
    }

    public static String queryLeases() {
        return one(QUERY_LEASES);
    }

    public static String queryEvents() {
        return one(QUERY_EVENTS);
    }

    /** 查询配额的 2 条索引，缺了只影响性能，调用方可单独吞异常。 */
    public static List<String> queryIndexes() {
        return statements(QUERY_INDEXES);
    }

    public static String batchRuns() {
        return one(BATCH_RUNS);
    }

    public static String batchRunsIndex() {
        return one(BATCH_RUNS_INDEX);
    }

    /** 老库升级用的列定义片段（不带分号，供 ALTER TABLE ... ADD COLUMN 拼接）。 */
    public static String batchRunsAddHandle() {
        return one(BATCH_RUNS_ADD_HANDLE);
    }

    public static String idempotency() {
        return one(IDEMPOTENCY);
    }

    public static String batchLogLines() {
        return one(BATCH_LOG_LINES);
    }

    public static String queryText() {
        return one(QUERY_TEXT);
    }

    private static String one(String resource) {
        List<String> statements = statements(resource);
        if (statements.size() != 1) {
            throw new IllegalStateException("状态库 DDL 模板应只有一条语句: " + resource);
        }
        return statements.get(0);
    }

    private static List<String> statements(String resource) {
        List<String> statements = SqlScript.split(SqlScript.readResource(resource));
        // 去掉文件头注释：列定义片段要拼进 ALTER TABLE ... ADD COLUMN，不能带注释；
        // 其余语句也只返回纯 SQL，便于日志与跨库执行。
        List<String> clean = new ArrayList<>();
        for (String stmt : statements) {
            String sql = SqlScript.stripComments(stmt).trim();
            if (!sql.isEmpty()) {
                clean.add(sql);
            }
        }
        if (clean.isEmpty()) {
            throw new IllegalStateException("状态库 DDL 模板为空: " + resource);
        }
        return clean;
    }
}
