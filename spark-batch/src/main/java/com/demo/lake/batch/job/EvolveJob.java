package com.demo.lake.batch.job;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.cli.Args;
import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.catalog.LakeTables;
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

    @Override
    public String name() {
        return LakeJobs.EVOLVE;
    }

    @Override
    public void run(SparkSession spark, PlatformConfig cfg, Args args) {
        TableRef silver = TableRef.of(cfg, LakeTables.SILVER_ORDERS);

        List<String> columns = Arrays.asList(spark.table(silver.fqn()).columns());
        if (!columns.contains("channel")) {
            System.out.println("[evolve] ALTER TABLE ADD COLUMNS channel（仅元数据变更）");
            spark.sql("ALTER TABLE " + silver.fqn() + " ADD COLUMNS (channel STRING COMMENT '下单渠道')");
        } else {
            System.out.println("[evolve] channel 列已存在，跳过加列");
        }

        System.out.println("[evolve] 回填 channel（CoW 会重写受影响的数据文件）");
        spark.sql("UPDATE " + silver.fqn() + " SET channel = "
                + "CASE pmod(hash(user_id), 3) "
                + "WHEN 0 THEN 'APP' WHEN 1 THEN 'MINI_APP' ELSE 'WEB' END");

        spark.sql("SELECT channel, count(*) AS cnt FROM " + silver.fqn() + " GROUP BY channel ORDER BY cnt DESC")
                .show(false);
        System.out.println("[evolve] 当前 schema：");
        spark.table(silver.fqn()).printSchema();
    }
}
