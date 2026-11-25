package com.demo.lake.batch;

import com.demo.lake.common.batch.BatchRunStatus;
import com.demo.lake.batch.job.EvolveJob;
import com.demo.lake.batch.job.IngestJob;
import com.demo.lake.batch.job.InitJob;
import com.demo.lake.batch.job.MaintainJob;
import com.demo.lake.batch.job.QualityJob;
import com.demo.lake.batch.job.ShowJob;
import com.demo.lake.batch.job.TransformJob;
import com.demo.lake.batch.job.VerifySqlJob;
import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.batch.RunIds;
import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 批处理统一入口。
 *
 * <pre>
 * mvn -pl spark-batch -am exec:java -Dexec.args="--job init"
 * mvn -pl spark-batch -am exec:java -Dexec.args="--job ingest --days 7 --orders-per-day 2000"
 * mvn -pl spark-batch -am exec:java -Dexec.args="--job transform"
 * mvn -pl spark-batch -am exec:java -Dexec.args="--job maintain"
 * mvn -pl spark-batch -am exec:java -Dexec.args="--job show"
 * </pre>
 *
 * <p>每次运行结束都会往 {@code ops.batch_runs} 落一行终态记录（可用 {@code --no-record} 关掉）。
 * 记账失败不会改变退出码：作业结果以真实退出码为准，旁路失败只打提示。</p>
 */
public final class BatchApp {

    private static final Map<String, LakeJob> JOBS = new LinkedHashMap<>();

    static {
        register(new InitJob());
        register(new IngestJob());
        register(new TransformJob());
        register(new EvolveJob());
        register(new MaintainJob());
        register(new ShowJob());
        register(new VerifySqlJob());
        register(new QualityJob());
    }

    private static void register(LakeJob job) {
        JOBS.put(job.name(), job);
    }

    public static void main(String[] argv) {
        Args args = Args.parse(argv);
        if (args.flag("help")) {
            printUsage();
            return;
        }

        // 命令行覆盖必须发生在配置加载之前
        if (args.has("warehouse")) {
            System.setProperty("platform.warehouse", args.get("warehouse"));
        }
        if (args.has("catalog-type")) {
            System.setProperty("platform.catalog.type", args.get("catalog-type"));
        }
        if (args.has("hive-uri")) {
            System.setProperty("platform.hive.uri", args.get("hive-uri"));
        }

        Path conf = args.has("conf") ? Paths.get(args.get("conf")) : null;
        PlatformConfig cfg = PlatformConfig.load(conf);

        List<String> plan = resolvePlan(args);
        String runId = RunIds.resolve(args);
        // 回填：作业内部（如 ingest 往 bronze 写 run_id）读到的必须是同一个 ID
        args.put(RunIds.ARG, runId);
        Instant startedAt = Instant.now();
        System.out.println("======================================================");
        System.out.println(" lakehouse batch | run=" + runId + " job=" + plan);
        System.out.println(" " + cfg.describe());
        System.out.println("======================================================");

        SparkSession spark = SparkSessionFactory.create(cfg, "lakehouse-batch");
        BatchRunStatus status = BatchRunStatus.SUCCEEDED;
        int exitCode = 0;
        String message = "完成";
        try {
            for (String name : plan) {
                LakeJob job = JOBS.get(name);
                long start = System.currentTimeMillis();
                System.out.printf("%n>>> [%s] 开始%n", name);
                job.run(spark, cfg, args);
                System.out.printf("<<< [%s] 完成，耗时 %.1fs%n", name, (System.currentTimeMillis() - start) / 1000.0);
            }
        } catch (Exception e) {
            status = BatchRunStatus.FAILED;
            exitCode = 1;
            message = "作业失败: " + e.getMessage();
            System.err.println("作业失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("批处理作业失败: " + plan, e);
        } finally {
            // 必须在 spark.stop() 之前写：记账要复用同一个 session
            RunRecordWriter.record(spark, cfg, args, runId, String.join(" ", argv),
                    status, exitCode, startedAt, message);
            if (!args.flag("keep-session")) {
                spark.stop();
            }
        }
    }

    private static List<String> resolvePlan(Args args) {
        String job = args.job();
        if (LakeJobs.ALL.equals(job)) {
            List<String> plan = new ArrayList<>(Arrays.asList(LakeJobs.INIT, LakeJobs.INGEST, LakeJobs.TRANSFORM));
            if (args.flag("evolve")) {
                plan.add(LakeJobs.EVOLVE);
            }
            plan.addAll(Arrays.asList(LakeJobs.MAINTAIN, LakeJobs.SHOW));
            return plan;
        }
        if (!JOBS.containsKey(job)) {
            throw new IllegalArgumentException("未知 job: " + job + "，可选: " + JOBS.keySet() + " / " + LakeJobs.ALL);
        }
        return List.of(job);
    }

    private static void printUsage() {
        System.out.println("用法: --job <" + String.join("|", JOBS.keySet()) + "|" + LakeJobs.ALL + "> [选项]");
        System.out.println();
        System.out.println("通用选项:");
        System.out.println("  --conf <file>         指定配置文件（默认 conf/platform.properties，存在才生效）");
        System.out.println("  --warehouse <uri>     覆盖仓库路径，如 s3a://lake/warehouse、/data/warehouse");
        System.out.println("  --catalog-type <t>    hadoop | hive");
        System.out.println("  --hive-uri <uri>      thrift://hms:9083");
        System.out.println("  --keep-session       结束后不调用 spark.stop()（调试用；job 之间本来就复用同一个 session）");
        System.out.println("  --run-id <id>         指定运行 ID（服务端拉起时由 BATCH_RUN_ID 环境变量注入）");
        System.out.println("  --no-record           不往 ops.batch_runs 写运行记录");
        System.out.println();
        System.out.println("ingest:");
        System.out.println("  --days <n>            生成最近 n 天数据，默认 7");
        System.out.println("  --orders-per-day <n>  每天订单数，默认 2000");
        System.out.println();
        System.out.println("maintain:");
        System.out.println("  --retain-last <n>     过期快照时保留最近 n 个，默认 5");
        System.out.println();
        System.out.println("verify-sql:");
        System.out.println("  --sql-file <path>     用 Spark 校验 ad-hoc SQL（跳过 @trino-only 语句）");
        System.out.println();
        System.out.println("quality:");
        System.out.println("  --inject-faults       演示用：先注入重复单 + 负金额，再跑检查（预期 FAILED）");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  --job all --days 7 --orders-per-day 3000");
    }

    private BatchApp() {
    }
}
