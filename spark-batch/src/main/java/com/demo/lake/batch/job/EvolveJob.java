package com.demo.lake.batch.job;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.cli.Args;
import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.Migrations;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

/**
 * 表结构演进演示：给 silver 加 channel 列并回填。
 * Iceberg 加列是纯元数据操作，不改写历史数据文件；
 * 老快照仍然只有旧列，Spark/Trino 读新快照时缺值列自动返回 NULL。
 */
public class EvolveJob implements LakeJob {

    private static final String CHANNEL_STATS_SQL =
            "SELECT channel, COUNT(*) AS cnt FROM %s GROUP BY channel ORDER BY cnt DESC";

    @Override
    public String name() {
        return LakeJobs.EVOLVE;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig platformConfig, Args args) {
        TableRef silver = TableRef.of(platformConfig, LakeTables.SILVER_ORDERS);

        List<String> columns = Arrays.asList(spark.table(silver.fqn()).columns());
        if (!columns.contains("channel")) {
            System.out.println("[evolve] ALTER TABLE ADD COLUMNS channel（仅元数据变更）");
            spark.sql(Migrations.addSilverChannel(platformConfig));
        } else {
            System.out.println("[evolve] channel 列已存在，跳过加列");
        }

        System.out.println("[evolve] 回填 channel（CoW 会重写受影响的数据文件）");
        spark.sql(Migrations.backfillSilverChannel(platformConfig));

        spark.sql(String.format(CHANNEL_STATS_SQL, silver.fqn())).show(false);
        System.out.println("[evolve] 当前 schema：");
        spark.table(silver.fqn()).printSchema();
    }
}
