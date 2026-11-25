package com.demo.lake.common.cli;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlParamsTest {

    @Test
    void parseParamsSplitsOnFirstEqualsAndTrims() {
        Map<String, String> params =
                SqlParams.parseParams(" snapshot_id = 123 , expr = a=b , broken ,=ignored ");

        assertEquals(2, params.size());
        assertEquals("123", params.get("snapshot_id"));
        assertEquals("a=b", params.get("expr"));
    }

    @Test
    void parseParamsHandlesNullOrBlank() {
        assertTrue(SqlParams.parseParams(null).isEmpty());
        assertTrue(SqlParams.parseParams("  ").isEmpty());
    }

    @Test
    void prepareRendersAndSkipsStatementsWithMissingParams() {
        String script = "SELECT ${a} FROM t; SELECT ${missing} FROM t;";

        List<String> statements = SqlParams.prepare(script, Collections.singletonMap("a", "1"), false);

        assertEquals(Collections.singletonList("SELECT 1 FROM t"), statements);
    }

    @Test
    void prepareAddsExplainOnlyWhenMissing() {
        List<String> statements =
                SqlParams.prepare("SELECT 1; EXPLAIN SELECT 2;", Collections.emptyMap(), true);

        assertEquals(2, statements.size());
        assertEquals("EXPLAIN SELECT 1", statements.get(0));
        assertEquals("EXPLAIN SELECT 2", statements.get(1));
    }
}
