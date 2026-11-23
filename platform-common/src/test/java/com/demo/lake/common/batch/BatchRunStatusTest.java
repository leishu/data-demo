package com.demo.lake.common.batch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchRunStatusTest {

    @Test
    void 五种状态都带中文描述() {
        assertEquals(5, BatchRunStatus.values().length);
        assertEquals("排队中", BatchRunStatus.QUEUED.description());
        assertEquals("运行中", BatchRunStatus.RUNNING.description());
        assertEquals("成功", BatchRunStatus.SUCCEEDED.description());
        assertEquals("失败", BatchRunStatus.FAILED.description());
        assertEquals("超时", BatchRunStatus.TIMEOUT.description());
    }

    @Test
    void 只有终态值得落表() {
        assertTrue(BatchRunStatus.SUCCEEDED.isTerminal());
        assertTrue(BatchRunStatus.FAILED.isTerminal());
        assertTrue(BatchRunStatus.TIMEOUT.isTerminal());
        assertFalse(BatchRunStatus.QUEUED.isTerminal());
        assertFalse(BatchRunStatus.RUNNING.isTerminal());
    }

    @Test
    void 宽松解析兼容大小写空白与未知值() {
        assertEquals(BatchRunStatus.SUCCEEDED, BatchRunStatus.from(" succeeded "));
        assertEquals(BatchRunStatus.RUNNING, BatchRunStatus.from("RUNNING"));
        assertNull(BatchRunStatus.from(null));
        assertNull(BatchRunStatus.from("UNKNOWN"));
    }
}
