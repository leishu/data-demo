package com.demo.lake.common.sql;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlScriptTest {

    @Test
    void splitsOnSemicolonsOutsideQuotesAndComments() {
        String script = ""
                + "-- 头部注释\n"
                + "SELECT ';' AS semi;  -- 行尾注释\n"
                + "\n"
                + "/* 块注释; 里的分号 */\n"
                + "SELECT count(*) FROM t;\n"
                + "SELECT 1\n";

        List<String> stmts = SqlScript.split(script);
        assertEquals(3, stmts.size());
        assertTrue(stmts.get(0).contains("AS semi"));
        assertTrue(stmts.get(1).contains("count(*)"));
        assertEquals("SELECT 1", stmts.get(2));
    }

    @Test
    void onlyCommentsProducesNoStatements() {
        assertTrue(SqlScript.split("-- 只有注释\n\n/* 还是注释 */\n").isEmpty());
    }

    @Test
    void oneLineCollapsesWhitespace() {
        assertEquals("SELECT 1 FROM t", SqlScript.oneLine("SELECT 1\n  FROM t -- 注释\n", 100));
        assertEquals("SELECT ...", SqlScript.oneLine("SELECT * FROM long_table", 6));
    }
}
