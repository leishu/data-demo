package com.demo.lake.common.batch;

import com.demo.lake.common.cli.Args;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * 批作业运行 ID 的唯一解析入口。
 *
 * <p>优先级：{@code --run-id} &gt; 环境变量 {@code BATCH_RUN_ID} &gt; 本进程生成。
 * 被 lake-service 拉起时用服务端注入的 ID，{@code ops.batch_runs} 与 bronze 数据行里的
 * run_id 才会指向同一个批次。</p>
 *
 * <p>自编时形如 {@code run-2026-11-19-13-45-9a1aad}（时间部分按 {@code uuuu-MM-dd-HH-mm} 格式化，
 * 使用 JVM 本地时区，与页面开始/结束时间（浏览器本地）同源，让人看到的 ID 时间和行上的开始时间一致）：
 * 分钟级时间戳只是给人看的，唯一性靠后面 6 位随机后缀（同一分钟内多次运行也不会撞）。
 * 服务端与 spark-batch 共用 {@link #generate()}，保证两边生成的 ID 格式一致。
 * epoch 毫秒（{@code ops.batch_runs.started_ms}）本身没有时区，解析和显示各自以绝对时刻为准。</p>
 */
public final class RunIds {

    /** 服务端拉起作业时注入的环境变量名 */
    public static final String ENV = "BATCH_RUN_ID";

    /** 命令行参数名；BatchApp 解析后会回填到 Args，作业侧直接读同一个键 */
    public static final String ARG = "run-id";

    /** run id 的时间部分精确到分钟；用 JVM 本地时区，让 ID 上的时间与页面显示的开始时间一致 */
    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd-HH-mm").withZone(ZoneId.systemDefault());

    public static String resolve(Args args) {
        String given = args.get(ARG);
        if (given != null && !given.isBlank()) {
            return given.trim();
        }
        String fromEnv = env(ENV);
        if (fromEnv != null) {
            return fromEnv;
        }
        return generate();
    }

    /** 按 {@code run-uuuu-MM-dd-HH-mm-xxxxxx} 生成一个新 ID：本地时区的分钟级时间戳 + 6 位随机后缀 */
    public static String generate() {
        return "run-" + TIME.format(Instant.now()) + "-" + UUID.randomUUID().toString().substring(0, 6);
    }

    private static String env(String key) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? null : v.trim();
    }

    private RunIds() {
    }
}