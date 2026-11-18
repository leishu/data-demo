package com.demo.lake.batch;

import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

/** 按平台配置构建 SparkSession（Iceberg catalog 由 spark.sql.catalog.* 注入） */
public final class SparkSessionFactory {

    private SparkSessionFactory() {
    }

    public static SparkSession create(PlatformConfig cfg, String appName) {
        SparkSession.Builder builder = SparkSession.builder()
                .appName(appName)
                .master(cfg.sparkMaster());

        boolean local = cfg.sparkMaster().startsWith("local");
        if (local) {
            // 本地模式没必要起 UI，也避免端口冲突；显式绑定回环地址防止反查主机名失败
            builder.config("spark.ui.enabled", "false")
                    .config("spark.driver.host", "127.0.0.1")
                    .config("spark.driver.bindAddress", "127.0.0.1");
        }

        for (Map.Entry<String, String> e : cfg.sparkConf().entrySet()) {
            builder.config(e.getKey(), e.getValue());
        }

        SparkSession spark = builder.getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        return spark;
    }
}
