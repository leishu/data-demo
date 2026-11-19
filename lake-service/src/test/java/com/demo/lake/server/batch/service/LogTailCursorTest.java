package com.demo.lake.server.batch.service;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 日志增量游标：只吐新增行，半行留到下一轮，中文与 CRLF 不能出错 */
class LogTailCursorTest {

    @Test
    void 只返回新增的完整行() throws Exception {
        Path file = Files.createTempFile("lake-log", ".log");
        Files.writeString(file, "a\nb\n");

        LogTailCursor cursor = new LogTailCursor();
        assertEquals(List.of("a", "b"), cursor.poll(file, 100));
        assertTrue(cursor.poll(file, 100).isEmpty(), "没有新内容时不该重复吐");

        Files.writeString(file, "c\npart", StandardOpenOption.APPEND);
        assertEquals(List.of("c"), cursor.poll(file, 100), "没等到换行的尾巴先攒着");

        Files.writeString(file, "ial\n", StandardOpenOption.APPEND);
        assertEquals(List.of("partial"), cursor.poll(file, 100));
    }

    @Test
    void 超过本次上限的行留到下次() throws Exception {
        Path file = Files.createTempFile("lake-log", ".log");
        Files.writeString(file, "1\n2\n3\n4\n");

        LogTailCursor cursor = new LogTailCursor();
        assertEquals(List.of("1", "2"), cursor.poll(file, 2));
        assertEquals(List.of("3", "4"), cursor.poll(file, 2));
    }

    @Test
    void 中文与CRLF都正常() throws Exception {
        Path file = Files.createTempFile("lake-log", ".log");
        Files.write(file, "作业完成\r\n行数 14000\r\n".getBytes(StandardCharsets.UTF_8));

        LogTailCursor cursor = new LogTailCursor();
        assertEquals(List.of("作业完成", "行数 14000"), cursor.poll(file, 10));
    }

    @Test
    void 预热只回放末尾指定行数() throws Exception {
        Path file = Files.createTempFile("lake-log", ".log");
        Files.writeString(file, "1\n2\n3\n4\n5\n");

        LogTailCursor cursor = new LogTailCursor();
        cursor.primeTail(file, 2);
        assertEquals(List.of("4", "5"), cursor.poll(file, 100));
        assertTrue(cursor.poll(file, 100).isEmpty(), "预热过的不该重复吐");

        Files.writeString(file, "6\n", StandardOpenOption.APPEND);
        assertEquals(List.of("6"), cursor.poll(file, 100));
    }

    @Test
    void 预热不包含末尾未换行的半行() throws Exception {
        Path file = Files.createTempFile("lake-log", ".log");
        Files.writeString(file, "a\nb\npart");

        LogTailCursor cursor = new LogTailCursor();
        cursor.primeTail(file, 10);
        assertEquals(List.of("a", "b"), cursor.poll(file, 100), "半行等下一轮补齐");

        Files.writeString(file, "ial\n", StandardOpenOption.APPEND);
        assertEquals(List.of("partial"), cursor.poll(file, 100));
    }

    @Test
    void 预热从大文件中间开始时丢弃被截断的首行() throws Exception {
        Path file = Files.createTempFile("lake-log", ".log");
        String big = "x".repeat(4000) + "\n";
        Files.writeString(file, big.repeat(100) + "last-1\nlast-2\n");

        LogTailCursor cursor = new LogTailCursor();
        cursor.primeTail(file, 2);
        assertEquals(List.of("last-1", "last-2"), cursor.poll(file, 10));
    }

    @Test
    void 文件被截断后从头重读() throws Exception {
        Path file = Files.createTempFile("lake-log", ".log");
        Files.writeString(file, "old-1\nold-2\n");

        LogTailCursor cursor = new LogTailCursor();
        assertEquals(List.of("old-1", "old-2"), cursor.poll(file, 10));

        // 轮转/覆盖：文件变小了，游标要重置而不是读越界
        Files.writeString(file, "new\n");
        assertEquals(List.of("new"), cursor.poll(file, 10));
    }
}
