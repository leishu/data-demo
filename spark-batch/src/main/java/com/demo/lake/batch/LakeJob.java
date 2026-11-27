package com.demo.lake.batch;

import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.SparkSession;

/** 批处理作业统一契约：一个 job 复用一个 SparkSession */
public interface LakeJob {

    String name();

    void run(SparkSession spark, PlatformConfig platformConfig, Args args) throws Exception;
}
