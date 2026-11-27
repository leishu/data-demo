package com.demo.lake.common.batch;

import com.demo.lake.common.cli.Args;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunIdsTest {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("uuuu-MM-dd-HH-mm");

    @Test
    void generate生成符合格式且时间接近当前本地时间() {
        String id = RunIds.generate();
        assertTrue(id.matches("run-\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-[0-9a-f]{12}"),
                "ID 形状不对: " + id);
        // 用 JVM 本地时区解析 ID 里的分钟级时间戳：回归到 UTC 会离当前本地时间差几小时，这里抓住
        Instant idTime = LocalDateTime.parse(id.substring(4, 20), TIME)
                .atZone(ZoneId.systemDefault()).toInstant();
        assertTrue(Duration.between(idTime, Instant.now()).abs().compareTo(Duration.ofMinutes(2)) <= 0,
                "ID 时间偏离当前本地时间过远: " + idTime);
    }

    @Test
    void generate在同一分钟内唯一() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            ids.add(RunIds.generate());
        }
        assertEquals(1000, ids.size());
    }

    @Test
    void resolve优先用命令行传入的ID() {
        assertEquals("given-id", RunIds.resolve(Args.parse(new String[]{"--run-id", "given-id"})));
    }
}
