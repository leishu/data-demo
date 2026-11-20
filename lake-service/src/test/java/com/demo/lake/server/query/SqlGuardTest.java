package com.demo.lake.server.query;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlGuardTest {

    @Test
    void 只读语句放行并去掉结尾分号() {
        assertEquals("select 1", SqlGuard.requireReadOnly("  select 1 ;  ", false));
        assertEquals("with t as (select 1) select * from t",
                SqlGuard.requireReadOnly("with t as (select 1) select * from t", false));
        assertEquals("show tables", SqlGuard.requireReadOnly("show tables", false));
    }

    @Test
    void 注释不影响首关键字判断() {
        String sql = "-- 每日营收\n/* 测试 */\nselect count(*) from gold";
        assertEquals("select count(*) from gold", SqlGuard.requireReadOnly(sql, false));
    }

    @Test
    void 写入语句默认拒绝() {
        InvalidQueryException e = assertThrows(InvalidQueryException.class,
                () -> SqlGuard.requireReadOnly("insert into t values (1)", false));
        assertTrue(e.getMessage().contains("只读"));
    }

    @Test
    void 允许写入时放行并且注释掉的写入不误判() {
        assertEquals("delete from t", SqlGuard.requireReadOnly("delete from t", true));
        // 注释里的 delete 先被剥掉，剩下的 select 仍是只读语句
        assertEquals("select 1", SqlGuard.requireReadOnly("-- delete from t\nselect 1", false));
    }

    @Test
    void 多语句拒绝() {
        assertThrows(InvalidQueryException.class,
                () -> SqlGuard.requireReadOnly("select 1; select 2", false));
    }

    @Test
    void 空语句拒绝() {
        assertThrows(InvalidQueryException.class, () -> SqlGuard.requireReadOnly("   ", false));
        assertThrows(InvalidQueryException.class, () -> SqlGuard.requireReadOnly("-- 只有注释", false));
    }

    @Test
    void 只读模式下EXPLAIN_ANALYZE的内层语句也要校验() {
        // ANALYZE 会真实执行内层语句，写入必须被拦下
        assertThrows(InvalidQueryException.class,
                () -> SqlGuard.requireReadOnly("explain analyze insert into t values (1)", false));
        assertThrows(InvalidQueryException.class,
                () -> SqlGuard.requireReadOnly("EXPLAIN (ANALYZE, FORMAT JSON) delete from t", false));
        assertThrows(InvalidQueryException.class,
                () -> SqlGuard.requireReadOnly("explain analyze verbose merge into t using s on t.id = s.id", false));

        // 内层仍是只读语句则放行；普通 EXPLAIN（只出计划、不执行）也不受影响
        assertEquals("explain analyze select 1",
                SqlGuard.requireReadOnly("explain analyze select 1", false));
        assertEquals("explain insert into t values (1)",
                SqlGuard.requireReadOnly("explain insert into t values (1)", false));

        // 显式允许写入时不必再拆内层
        assertEquals("explain analyze insert into t values (1)",
                SqlGuard.requireReadOnly("explain analyze insert into t values (1)", true));
    }

    @Test
    void 首关键字提取() {
        assertEquals("select", SqlGuard.firstKeyword("SELECT * from t"));
        assertEquals("describe", SqlGuard.firstKeyword("\n\t describe iceberg.lake.t"));
    }
}
