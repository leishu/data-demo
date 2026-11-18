package com.demo.lake.service.query;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.service.config.LakeProperties;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 连不上 Presto 时必须抛 {@link PrestoUnavailableException}（映射 503），
 * 而不是普通的 SQL 执行失败（映射 400）。这里连一个必然拒绝的端口来复现。
 */
class QueryServiceTest {

    private QueryService serviceAgainstDeadPort() {
        HikariConfig hc = new HikariConfig();
        hc.setPoolName("dead-presto");
        hc.setJdbcUrl("jdbc:presto://127.0.0.1:1/iceberg/lake");
        hc.setUsername("tester");
        hc.setDriverClassName("com.facebook.presto.jdbc.PrestoDriver");
        hc.setConnectionTimeout(800);
        hc.setInitializationFailTimeout(-1);
        HikariDataSource ds = new HikariDataSource(hc);
        return new QueryService(ds, new LakeProperties(), PlatformConfig.load());
    }

    @Test
    void 连接失败归类为依赖不可用() {
        QueryService service = serviceAgainstDeadPort();
        RuntimeException e = assertThrows(RuntimeException.class, () -> service.query("select 1", 10));
        System.out.println("[诊断] 抛出的类型 = " + e.getClass().getName()
                + " / cause = " + (e.getCause() == null ? "null" : e.getCause().getClass().getName())
                + " / sqlState = " + (e.getCause() instanceof java.sql.SQLException se ? se.getSQLState() : "-"));
        assertThrows(PrestoUnavailableException.class, () -> service.query("select 1", 10));
    }
}
