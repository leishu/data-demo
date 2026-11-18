package com.demo.lake.server.config;

import com.demo.lake.common.config.PlatformConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;

/**
 * 把 platform-common 的配置体系接进 Spring 容器：
 * 全项目只有 {@link PlatformConfig} 一个配置来源，Spring 这边只是把它变成可注入的 bean。
 */
@Configuration
@EnableConfigurationProperties(LakeProperties.class)
@Slf4j
public class LakeConfiguration {

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
     * 阻塞操作的执行器。
     *
     * <p>WebFlux 的请求都跑在 Reactor Netty 的事件循环上，在那上面调 {@code getConnection()}、
     * {@code rs.next()}、{@code process.waitFor()} 会把 IO 线程池整个堵死——一个慢查询就能让服务不响应。
     * 所以这类调用统一包成 Mono/Flux 后 {@code subscribeOn} 到这里。</p>
     *
     * <p>用虚拟线程而不是固定大小线程池：阻塞 JDBC 与子进程等待都是「线程在睡等结果」，
     * 虚拟线程在这种情况下几乎不占平台线程，也不用为并发数去猜池大小。</p>
     */
    @Bean(destroyMethod = "dispose")
    public Scheduler blockingScheduler() {
        return Schedulers.fromExecutorService(
                Executors.newVirtualThreadPerTaskExecutor(), "lake-blocking");
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
        // Presto 0.290 的 JDBC 驱动没有 source 连接属性（会报 Unrecognized connection property），
        // 用 applicationNamePrefix 给驱动默认的 "presto-jdbc" 加前缀，效果等价于自定义 source。
        hikari.addDataSourceProperty("applicationNamePrefix", cfg.prestoSource() + "-service-");
        log.info("Presto 连接池 -> {} (pool={})", cfg.prestoJdbcUrl(), props.getQuery().getPoolSize());
        return new HikariDataSource(hikari);
    }
}
