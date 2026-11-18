package com.demo.lake.service.config;

import com.demo.lake.common.config.PlatformConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 把 platform-common 的配置体系接进 Spring 容器：
 * 全项目只有 {@link PlatformConfig} 一个配置来源，Spring 这边只是把它变成可注入的 bean。
 */
@Configuration
@EnableConfigurationProperties(LakeProperties.class)
public class LakeConfiguration {

    private static final Logger log = LoggerFactory.getLogger(LakeConfiguration.class);

    @Bean
    public PlatformConfig platformConfig(LakeProperties props) {
        Path conf = null;
        String configured = props.getConfFile();
        if (configured != null && !configured.isBlank()) {
            Path p = Paths.get(configured);
            if (Files.isReadable(p)) {
                conf = p;
            } else {
                log.warn("配置文件 {} 不存在，改用 classpath 默认值 + 环境变量 LAKE_*", configured);
            }
        }
        PlatformConfig cfg = PlatformConfig.load(conf);
        log.info("平台配置 -> {}", cfg.describe());
        return cfg;
    }

    /**
     * Presto 连接池。初始化失败不阻止应用启动（initializationFailTimeout = -1）：
     * 服务要能在 Presto 没起来时先提供 /actuator/health 和明确的错误信息，而不是起不来。
     */
    @Bean(destroyMethod = "close")
    public DataSource dataSource(PlatformConfig cfg, LakeProperties props) {
        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("presto");
        hikari.setJdbcUrl(cfg.prestoJdbcUrl());
        hikari.setUsername(cfg.prestoUser());
        hikari.setDriverClassName("com.facebook.presto.jdbc.PrestoDriver");
        hikari.setReadOnly(true);
        hikari.setMaximumPoolSize(props.getQuery().getPoolSize());
        hikari.setConnectionTimeout(props.getQuery().getConnectionTimeoutMs());
        hikari.setInitializationFailTimeout(-1);
        // Presto 侧按 source 统计来源，便于在 query 历史里区分服务和 CLI
        hikari.addDataSourceProperty("source", cfg.prestoSource() + "-service");
        log.info("Presto 连接池 -> {} (pool={})", cfg.prestoJdbcUrl(), props.getQuery().getPoolSize());
        return new HikariDataSource(hikari);
    }
}
