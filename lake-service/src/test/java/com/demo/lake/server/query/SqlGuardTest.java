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
    void 首关键字提取() {
        assertEquals("select", SqlGuard.firstKeyword("SELECT * from t"));
        assertEquals("describe", SqlGuard.firstKeyword("\n\t describe iceberg.lake.t"));
    }
}
