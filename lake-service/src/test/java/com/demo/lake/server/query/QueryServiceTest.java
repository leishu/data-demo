package com.demo.lake.server.query;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.query.model.QueryResult;
import com.demo.lake.server.query.service.QueryService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.core.scheduler.Schedulers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 连不上 Trino 时必须抛 {@link TrinoUnavailableException}（映射 503），
 * 而不是普通的 SQL 执行失败（映射 400）。这里连一个必然拒绝的端口来复现。
 *
 * <p>服务已经是响应式的，测试用 {@code Schedulers.immediate()} 让订阅同步执行，
 * 再 {@code block()} 拿结果——不额外引 reactor-test。</p>
 */
class QueryServiceTest {

    private QueryService serviceAgainstDeadPort() {
        HikariConfig hc = new HikariConfig();
        hc.setPoolName("dead-trino");
        hc.setJdbcUrl("jdbc:trino://127.0.0.1:1/iceberg/lake");
        hc.setUsername("tester");
        hc.setDriverClassName("io.trino.jdbc.TrinoDriver");
        hc.setConnectionTimeout(800);
        hc.setInitializationFailTimeout(-1);
        HikariDataSource ds = new HikariDataSource(hc);
        return new QueryService(new JdbcTemplate(ds), new LakeProperties(), PlatformConfig.load(), Schedulers.immediate());
    }

    @Test
    void 连接失败归类为依赖不可用() {
        QueryService service = serviceAgainstDeadPort();
        RuntimeException e = assertThrows(RuntimeException.class, () -> service.query("select 1", 10).block());
        System.out.println("[诊断] 抛出的类型 = " + e.getClass().getName()
                + " / cause = " + (e.getCause() == null ? "null" : e.getCause().getClass().getName())
                + " / sqlState = " + (e.getCause() instanceof java.sql.SQLException se ? se.getSQLState() : "-"));
        assertThrows(TrinoUnavailableException.class, () -> service.query("select 1", 10).block());
    }

    /** SSE 那条路径不能把连接失败变成 200 + 空流，必须落成 error 事件（也就是同样的异常） */
    @Test
    void 流式查询的连接失败也归为依赖不可用() {
        QueryService service = serviceAgainstDeadPort();
        assertThrows(TrinoUnavailableException.class, () -> service.streamQuery("select 1", 10).blockLast());
    }

    /** 带参走 PreparedStatement，值按参数绑定；无参仍走 Statement（即席查询可能是 SHOW/EXPLAIN） */
    @Test
    void 参数绑定按值传入而不是拼进SQL() {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl("jdbc:h2:mem:trino-params;DB_CLOSE_DELAY=-1");
        HikariDataSource ds = new HikariDataSource(hc);
        QueryService service = new QueryService(new JdbcTemplate(ds), new LakeProperties(),
                PlatformConfig.load(), Schedulers.immediate());

        QueryResult bound = service.execute("SELECT ? AS v", null, "O'Reilly");
        assertEquals("O'Reilly", bound.rows().getFirst().getFirst());

        QueryResult plain = service.execute("SELECT 'plain' AS v", null);
        assertEquals("plain", plain.rows().getFirst().getFirst());
    }
}
