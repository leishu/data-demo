package com.demo.lake.server.support;

/**
 * 状态库表名（lake_batch / lake_audit）的单一事实源。
 *
 * <p>各 Store 里的私有 TABLE 常量都指向这里；外置 DDL 模板的表名由单测与本类校对，
 * 避免改名时 Java 和 SQL 各改一半。</p>
 */
public final class StateDbTables {

    public static final String BATCH_SCHEMA = "lake_batch";
    public static final String AUDIT_SCHEMA = "lake_audit";

    public static final String BATCH_RUNS = "lake_batch.batch_runs";
    public static final String BATCH_LOG_LINES = "lake_batch.batch_log_lines";
    public static final String IDEMPOTENCY = "lake_batch.idempotency";
    public static final String SERVER_LOCKS = "lake_batch.server_locks";
    public static final String QUERY_LEASES = "lake_batch.query_leases";
    public static final String QUERY_EVENTS = "lake_batch.query_events";
    public static final String QUERY_TEXT = "lake_audit.query_text";

    private StateDbTables() {
    }
}
