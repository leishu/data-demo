package com.demo.lake.batch.job;

import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlScript;
import com.demo.lake.common.sql.TrinoDialect;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * 用 Spark SQL 跑一遍 ad-hoc 脚本，做"没装 Trino 也能验证 SQL"的兜底。
 * <ul>
 *   <li>带 {@code @trino-only} 标记的语句（Trino 专有语法）会被跳过；</li>
 *   <li>其余语句做最小方言适配（date_add / INTERVAL 字面量）后交给 Spark 执行。</li>
 * </ul>
 */
public class VerifySqlJob implements LakeJob {

    private static final String SKIP_MARK = "@trino-only";

    @Override
    public String name() {
        return LakeJobs.VERIFY_SQL;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig cfg, Args args) {
        String file = args.get("sql-file", "sql/adhoc/01_daily_revenue.sql");
        Path path = Paths.get(file);
        if (!Files.isReadable(path)) {
            throw new IllegalArgumentException("SQL 文件不存在: " + path.toAbsolutePath());
        }

        System.out.println("[verify-sql] " + path.toAbsolutePath());
        List<String> statements = SqlScript.split(SqlScript.read(path));
        int ran = 0;
        int skipped = 0;
        int failed = 0;

        for (String stmt : statements) {
            if (stmt.contains(SKIP_MARK)) {
                skipped++;
                System.out.println("  -- 跳过(Trino 专有): " + SqlScript.oneLine(stmt, 90));
                continue;
            }
            String sql = TrinoDialect.toSpark(stmt);
            System.out.println("  >> " + SqlScript.oneLine(sql, 110));
            try {
                spark.sql(sql).show(20, false);
                ran++;
            } catch (Exception e) {
                failed++;
                System.out.println("  !! 执行失败: " + e.getMessage());
            }
        }
        System.out.printf("[verify-sql] 执行 %d 条，跳过 %d 条，失败 %d 条%n", ran, skipped, failed);
        if (failed > 0) {
            throw new IllegalStateException("verify-sql 有 " + failed + " 条失败");
        }
    }
}
