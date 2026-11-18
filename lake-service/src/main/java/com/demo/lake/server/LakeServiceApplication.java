package com.demo.lake.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 湖仓服务层入口（JDK 21 + Spring Boot 4 + logback）。
 *
 * <p>职责边界：</p>
 * <ul>
 *   <li>即席查询：把 Presto 的 JDBC 能力包成 REST 接口，连接走 HikariCP 池</li>
 *   <li>作业编排：按需触发 Spark 批作业（独立进程，避免在 Web 容器里养一个 SparkSession）</li>
 * </ul>
 *
 * <p>Spark 作业代码本身不引入 Spring：它要进 spark-submit 的 jar 形态、要能序列化到 executor，
 * 在 driver 里塞一个 ApplicationContext 只会让集群环境更难排查。</p>
 */
@SpringBootApplication
public class LakeServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(LakeServiceApplication.class, args);
    }
}
