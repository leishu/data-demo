package com.demo.lake.batch;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;

/**
 * 把一次运行落成 {@code ops.batch_runs} 的一行终态记录。
 *
 * <p>写账是旁路：表没建、catalog 挂了、权限不对，都只打一行提示，绝不改变作业退出码——
 * 「作业跑成功了但记账失败」和「作业失败」是两件事，不能混成一个非 0 退出码。</p>
 *
 * <p>不带 run_id 时由本进程自己编一个；被 lake-service 拉起时会通过环境变量
 * {@code BATCH_RUN_ID} / {@code BATCH_RUN_LOG} 注入服务端的运行 ID 与日志路径，
 * 这样服务端重启后从表里读回来的记录还能和原来的日志文件对上。</p>
 */
final class RunRecordWriter {

    /** message 可能是一整段堆栈摘要，截断到能放下的长度即可 */
    private static final int MAX_MESSAGE = 500;
    private static final int MAX_COMMAND = 500;

    private RunRecordWriter() {
    }

    static void record(SparkSession spark, PlatformConfig cfg, Args args, String runId, String command,
                       String status, int exitCode, Instant startedAt, String message) {
        if (args.flag("no-record")) {
            System.out.println("[run-record] 已跳过（--no-record）");
            return;
        }
        Instant finishedAt = Instant.now();
        long durationMs = finishedAt.toEpochMilli() - startedAt.toEpochMilli();
        TableRef table = TableRef.of(cfg.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.BATCH_RUNS);
        try {
            StructType schema = new StructType()
                    .add("run_id", DataTypes.StringType, false)
                    .add("job", DataTypes.StringType, false)
                    .add("command", DataTypes.StringType, true)
                    .add("status", DataTypes.StringType, false)
                    .add("exit_code", DataTypes.IntegerType, true)
                    .add("started_at", DataTypes.TimestampType, false)
                    .add("finished_at", DataTypes.TimestampType, false)
                    .add("duration_ms", DataTypes.LongType, false)
                    .add("log_file", DataTypes.StringType, true)
                    .add("message", DataTypes.StringType, true);

            Row row = RowFactory.create(
                    runId,
                    args.job(),
                    truncate(command, MAX_COMMAND),
                    status,
                    exitCode,
                    Timestamp.from(startedAt),
                    Timestamp.from(finishedAt),
                    durationMs,
                    env("BATCH_RUN_LOG"),
                    truncate(message, MAX_MESSAGE));

            spark.createDataFrame(Collections.singletonList(row), schema)
                    .writeTo(table.fqn())
                    .append();
            System.out.printf("[run-record] 已写入 %s run_id=%s status=%s%n", table.fqn(), runId, status);
        } catch (Exception e) {
            // 明确告诉人「这不是作业失败」，否则看日志的人会以为要重跑
            System.out.println("[run-record] 写入 " + table.fqn() + " 失败（不影响作业结果）: " + e.getMessage());
        }
    }

    static String env(String key) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? null : v.trim();
    }

    private static String truncate(String text, int max) {
        if (text == null) {
            return null;
        }
        String single = text.replaceAll("\\s+", " ").trim();
        return single.length() <= max ? single : single.substring(0, max - 3) + "...";
    }
}
