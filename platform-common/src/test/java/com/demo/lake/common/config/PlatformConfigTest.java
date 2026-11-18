package com.demo.lake.common.config;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlatformConfigTest {

    @Test
    void defaultsAreHadoopCatalogWithLocalWarehouse() {
        PlatformConfig cfg = PlatformConfig.load();

        assertEquals("iceberg", cfg.catalogName());
        assertEquals("hadoop", cfg.catalogType());
        assertFalse(cfg.isHiveCatalog());
        assertTrue(cfg.warehouse().startsWith("file://"), "相对路径应被解析成 file:// 绝对路径");
        assertEquals(2, cfg.formatVersion());
    }

    @Test
    void systemPropertyOverridesAndHiveConf() {
        System.setProperty("platform.catalog.type", "hive");
        System.setProperty("platform.hive.uri", "thrift://hms.demo:9083");
        System.setProperty("platform.warehouse", "s3a://lake/warehouse");
        try {
            PlatformConfig cfg = PlatformConfig.load();
            assertTrue(cfg.isHiveCatalog());
            assertTrue(cfg.isObjectStorage());

            Map<String, String> conf = cfg.sparkConf();
            assertEquals("org.apache.iceberg.spark.SparkCatalog", conf.get("spark.sql.catalog.iceberg"));
            assertEquals("hive", conf.get("spark.sql.catalog.iceberg.type"));
            assertEquals("thrift://hms.demo:9083", conf.get("spark.sql.catalog.iceberg.uri"));
            assertEquals("s3a://lake/warehouse/", conf.get("spark.sql.catalog.iceberg.warehouse"));
            assertEquals("org.apache.iceberg.aws.s3.S3FileIO", conf.get("spark.sql.catalog.iceberg.io-impl"));
            assertEquals("dynamic", conf.get("spark.sql.sources.partitionOverwriteMode"));
        } finally {
            System.clearProperty("platform.catalog.type");
            System.clearProperty("platform.hive.uri");
            System.clearProperty("platform.warehouse");
        }
    }

    @Test
    void prestoUrlFollowsCatalogAndDatabase() {
        System.setProperty("platform.database", "lake_test");
        try {
            PlatformConfig cfg = PlatformConfig.load();
            assertEquals("jdbc:presto://localhost:8080/iceberg/lake_test", cfg.prestoJdbcUrl());
        } finally {
            System.clearProperty("platform.database");
        }
    }

    @Test
    void absoluteWarehouseIsKeptAsUri() {
        String abs = Paths.get("/tmp/lake-warehouse").toUri().toString();
        System.setProperty("platform.warehouse", abs);
        try {
            PlatformConfig cfg = PlatformConfig.load();
            assertEquals(abs.endsWith("/") ? abs : abs + "/", cfg.warehouse());
        } finally {
            System.clearProperty("platform.warehouse");
        }
    }
}
