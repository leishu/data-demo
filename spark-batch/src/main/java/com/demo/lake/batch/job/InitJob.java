package com.demo.lake.batch.job;

import com.demo.lake.common.cli.Args;
import com.demo.lake.batch.LakeJob;
import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlScript;
import org.apache.spark.sql.SparkSession;

/** 建库建表：ODS / DWD / ADS 三层。幂等，可重复执行 */
public class InitJob implements LakeJob {

    @Override
    public String name() {
        return "init";
    }

    @Override
    public void run(SparkSession spark, PlatformConfig cfg, Args args) {
        System.out.println("[init] catalog=" + cfg.catalogName() + " database=" + cfg.database()
                + " warehouse=" + cfg.warehouse());

        for (String ddl : LakeTables.all(cfg)) {
            System.out.println("  DDL> " + SqlScript.oneLine(ddl, 110));
            spark.sql(ddl);
        }

        System.out.println("[init] 当前库表：");
        spark.sql("SHOW TABLES IN " + cfg.catalogName() + "." + cfg.database()).show(false);
    }
}
