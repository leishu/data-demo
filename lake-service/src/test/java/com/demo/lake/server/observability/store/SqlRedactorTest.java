package com.demo.lake.server.observability.store;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlRedactorTest {

    @Test
    void 保留结构日期数字与短标识() {
        String sql = "select * from gold where dt = '2026-11-20' and merchant_id = 'M1' and amount > 100.5";
        String redacted = SqlRedactor.redact(sql);

        assertTrue(redacted.contains("dt = '2026-11-20'"));
        assertTrue(redacted.contains("merchant_id = 'M1'"));
        assertTrue(redacted.contains("amount > 100.5"));
        assertTrue(redacted.contains("select * from gold"));
    }

    @Test
    void 敏感上下文的字符串被掩码() {
        String redacted = SqlRedactor.redact(
                "select * from users where password = 'p@ssw0rd' and phone = '13800138000' and token = 'abc'");

        assertTrue(redacted.contains("'***'"));
        assertFalse(redacted.contains("p@ssw0rd"));
        assertFalse(redacted.contains("13800138000"));
        assertFalse(redacted.contains("'abc'"));
        // 别名出现在字面量之后也要识别
        String aliased = SqlRedactor.redact("select 'hunter2' as pwd, date '2026-11-20' as dt from t");
        assertFalse(aliased.contains("hunter2"), aliased);
        assertTrue(aliased.contains("'2026-11-20'"), aliased);
    }

    @Test
    void 注释被去掉转义引号不会破坏扫描() {
        String redacted = SqlRedactor.redact("-- note: secret=abc\nselect 'O''Brien' /* password=xyz */ from t");
        assertTrue(redacted.startsWith("select"));
        assertFalse(redacted.contains("secret"));
        assertFalse(redacted.contains("O'Brien"));
        assertTrue(redacted.contains("from t"));
    }
}
