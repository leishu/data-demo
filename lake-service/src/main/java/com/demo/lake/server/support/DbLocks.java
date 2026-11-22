package com.demo.lake.server.support;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Arrays;

/**
 * 基于数据库行的跨实例互斥锁。
 *
 * <p>不用 {@code pg_advisory_lock} 是为了让同一套 SQL 也能跑在单测的 H2 上：
 * 每张锁表只有固定的几行，谁先 {@code SELECT ... FOR UPDATE} 到某一行，谁就拿到该主题的全局锁，
 * 事务结束时自动释放。比进程内锁慢一点，但换来的是「多个服务实例共用同一份额度/幂等键」。</p>
 */
public final class DbLocks {

    public static final String TABLE = "lake_batch.server_locks";

    /** 各主题在锁表里对应的固定行名。用到的地方必须在这里登记，便于 init 一次建好。 */
    public static final String SUBMIT = "batch-submit";
    public static final String DISPATCH = "batch-dispatch";
    public static final String QUERY_QUOTA = "query-quota";
    public static final String LOG_APPEND = "log-append";

    private DbLocks() {
    }

    /** 建表并补齐固定锁行；重复调用安全。 */
    public static void ensure(DataSource dataSource, String... names) {
        try (Connection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS lake_batch");
            stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (name VARCHAR(64) PRIMARY KEY)");
            for (String name : names) {
                insert(conn, name);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("初始化数据库锁失败: " + e.getMessage(), e);
        }
    }

    /**
     * 拿到主题锁后才返回；调用方必须自己管理事务，锁在 commit/rollback 时释放。
     */
    public static void lock(Connection conn, String name) throws SQLException {
        insert(conn, name); // 首次使用时按需补行
        try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM " + TABLE + " WHERE name=? FOR UPDATE")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("数据库锁行丢失: " + name);
                }
            }
        }
    }

    /** 插入锁行，已存在就忽略；在事务里用 savepoint 避免 Postgres 把事务标记为 aborted。 */
    private static void insert(Connection conn, String name) throws SQLException {
        Savepoint savepoint = conn.getAutoCommit() ? null : conn.setSavepoint();
        try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + TABLE + " (name) VALUES (?)")) {
            ps.setString(1, name);
            ps.executeUpdate();
        } catch (SQLException duplicate) {
            if (savepoint == null) {
                // 自动提交：重复是预期的，直接忽略
                return;
            }
            conn.rollback(savepoint);
        } finally {
            if (savepoint != null) {
                conn.releaseSavepoint(savepoint);
            }
        }
    }

    public static String[] allLocks() {
        return Arrays.copyOf(new String[]{SUBMIT, DISPATCH, QUERY_QUOTA, LOG_APPEND}, 4);
    }
}
