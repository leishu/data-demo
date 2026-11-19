package com.demo.lake.common.batch;

import com.demo.lake.common.cli.Args;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * 批作业运行 ID 的唯一解析入口。
 *
 * <p>优先级：{@code --run-id} &gt; 环境变量 {@code BATCH_RUN_ID} &gt; 本进程生成。
 * 被 lake-service 拉起时用服务端注入的 ID，{@code ops.batch_runs} 与 bronze 数据行里的
 * run_id 才会指向同一个批次。</p>
 *
 * <p>本进程自编时形如 {@code run-2026-11-19-13:45-9a1aad}：分钟级时间戳只是给人看的，
 * 唯一性靠后面 6 位随机后缀（同一分钟内多次运行也不会撞）。</p>
 */
public final class RunIds {

    /** 服务端拉起作业时注入的环境变量名 */
    public static final String ENV = "BATCH_RUN_ID";

    /** 命令行参数名；BatchApp 解析后会回填到 Args，作业侧直接读同一个键 */
    public static final String ARG = "run-id";

    /** run id 的时间部分精确到分钟；固定 UTC，与 ops.batch_runs.started_at 同一时间基准 */
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd-HH-mm").withZone(ZoneOffset.UTC);

    public static String resolve(Args args) {
        String given = args.get(ARG);
        if (given != null && !given.isBlank()) {
            return given.trim();
        }
        String fromEnv = env(ENV);
        if (fromEnv != null) {
            return fromEnv;
        }
        return "run-" + TIME.format(Instant.now()) + "-" + UUID.randomUUID().toString().substring(0, 6);
    }

    private static String env(String key) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? null : v.trim();
    }

    private RunIds() {
    }
}