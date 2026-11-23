package com.demo.lake.server.batch.service;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** H2 验证共享日志表的追加、增量读取、尾部截断与过期清理 */
class JdbcLogSinkTest {

    private JdbcLogSink sink;

    @BeforeEach
    void setUp() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:logs-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        sink = new JdbcLogSink(new JdbcTemplate(ds),
                new TransactionTemplate(new DataSourceTransactionManager(ds)));
        sink.init();
    }

    @Test
    void 追加tail与增量读取() {
        sink.append("r1", List.of("line-1", "line-2"));
        assertEquals(List.of("line-1", "line-2"), texts(sink.tail("r1", 10)));
        assertEquals(List.of(), texts(sink.since("r1", 2, 10)));

        sink.append("r1", List.of("line-3"));
        assertEquals(List.of("line-3"), texts(sink.since("r1", 2, 10)));
        assertEquals(List.of("line-2", "line-3"), texts(sink.tail("r1", 2)));
    }

    @Test
    void 只保留最近两千行() {
        List<String> many = IntStream.range(0, 2010).mapToObj(i -> "L" + i).toList();
        sink.append("r2", many);

        List<LogSink.LogLine> tail = sink.tail("r2", 5000);
        assertEquals(2000, tail.size());
        assertEquals("L10", tail.get(0).text());
        assertEquals("L2009", tail.get(tail.size() - 1).text());
    }

    @Test
    void 按时间清理过期日志() {
        sink.append("r3", List.of("old"));
        assertEquals(0, sink.purge(Instant.now().minus(1, ChronoUnit.DAYS)));
        assertEquals(1, sink.purge(Instant.now().plus(1, ChronoUnit.DAYS)));
        assertEquals(List.of(), sink.tail("r3", 10));
    }

    private static List<String> texts(List<LogSink.LogLine> lines) {
        return lines.stream().map(LogSink.LogLine::text).toList();
    }
}
