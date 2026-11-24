package com.demo.lake.server.support;

import com.demo.lake.common.sql.SqlScript;
import com.demo.lake.common.sql.SqlTemplate;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 状态库 DDL 模板的守卫：模板漂移、表名与 Java 常量不一致都会在这里挂掉。 */
class StateDbDdlTest {

    @Test
    void everyTemplateLoadsAndSplitsIntoStatements() {
        for (String resource : StateDbDdl.all()) {
            String script = SqlScript.readResource(resource);
            assertFalse(script.trim().isEmpty(), resource + " 为空");
            assertFalse(SqlTemplate.hasUnresolved(script), resource + " 残留未解析占位符");
            assertFalse(SqlScript.split(script).isEmpty(), resource + " 没有可执行语句");
        }
    }

    @Test
    void tableNamesMatchJavaConstants() {
        assertTrue(StateDbDdl.schemaBatch().contains(StateDbTables.BATCH_SCHEMA));
        assertTrue(StateDbDdl.schemaAudit().contains(StateDbTables.AUDIT_SCHEMA));
        assertTrue(StateDbDdl.serverLocks().contains(StateDbTables.SERVER_LOCKS));
        assertTrue(StateDbDdl.queryLeases().contains(StateDbTables.QUERY_LEASES));
        assertTrue(StateDbDdl.queryEvents().contains(StateDbTables.QUERY_EVENTS));
        assertTrue(StateDbDdl.batchRuns().contains(StateDbTables.BATCH_RUNS));
        assertTrue(StateDbDdl.idempotency().contains(StateDbTables.IDEMPOTENCY));
        assertTrue(StateDbDdl.batchLogLines().contains(StateDbTables.BATCH_LOG_LINES));
        assertTrue(StateDbDdl.queryText().contains(StateDbTables.QUERY_TEXT));

        List<String> indexes = StateDbDdl.queryIndexes();
        assertTrue(indexes.size() == 2);
        assertTrue(indexes.get(0).contains(StateDbTables.QUERY_EVENTS));
        assertTrue(indexes.get(1).contains(StateDbTables.QUERY_LEASES));
        assertTrue(StateDbDdl.batchRunsIndex().contains(StateDbTables.BATCH_RUNS));
    }

    @Test
    void addHandleColumnDefinitionMatchesCreateTable() {
        String definition = StateDbDdl.batchRunsAddHandle().trim();
        assertTrue(definition.equals("handle VARCHAR(255)"), "迁移列定义变了: " + definition);
        assertTrue(StateDbDdl.batchRuns().contains(definition), "建表与迁移的列定义不一致");
        assertFalse(definition.contains(";"), "列定义片段不能带分号");
    }
}
