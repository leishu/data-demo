package com.demo.lake.server.config;

import com.demo.lake.common.config.PlatformConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
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
     * Trino 连接池。初始化失败不阻止应用启动（initializationFailTimeout = -1）：
     * 服务要能在 Trino 没起来时先提供 /actuator/health 和明确的错误信息，而不是起不来。
     */
    @Bean(name = "trinoDataSource", destroyMethod = "close")
    public HikariDataSource trinoDataSource(PlatformConfig cfg, LakeProperties props) {
        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("trino");
        hikari.setJdbcUrl(cfg.trinoJdbcUrl());
        hikari.setUsername(cfg.trinoUser());
        hikari.setDriverClassName("io.trino.jdbc.TrinoDriver");
        hikari.setReadOnly(true);
        hikari.setMaximumPoolSize(props.getQuery().getPoolSize());
        hikari.setConnectionTimeout(props.getQuery().getConnectionTimeoutMs());
        hikari.setInitializationFailTimeout(-1);
        // source 是 Trino 驱动认的连接属性，会在 coordinator 的查询列表里标识来源
        hikari.addDataSourceProperty("source", cfg.trinoSource() + "-service");
        // 会话时区固定 UTC：Spark 把时间戳按 UTC 墙钟写进湖表，若会话用机器时区（如 Asia/Shanghai），
        // to_unixtime 等换算会整体偏移 8 小时，历史记录还会和内存里的 Instant 错序
        hikari.addDataSourceProperty("timezone", "UTC");
        log.info("Trino 连接池 -> {} (pool={})", cfg.trinoJdbcUrl(), props.getQuery().getPoolSize());
        return new HikariDataSource(hikari);
    }

    /**
     * 运行状态库（Postgres）：批作业的运行态与心跳都放这里，服务本身无状态。
     * 初始化失败不阻止启动：没起 infra 时服务仍能提供只读查询，批作业接口返回明确的 503。
     */
    @Bean(name = "stateDataSource", destroyMethod = "close")
    public HikariDataSource stateDataSource(LakeProperties props) {
        LakeProperties.Batch.State state = props.getBatch().getState();
        HikariConfig hikari = new HikariConfig();
        hikari.setPoolName("batch-state");
        hikari.setJdbcUrl(state.getUrl());
        hikari.setUsername(state.getUser());
        hikari.setPassword(state.getPassword());
        hikari.setDriverClassName("org.postgresql.Driver");
        hikari.setMaximumPoolSize(state.getPoolSize());
        hikari.setInitializationFailTimeout(-1);
        log.info("运行状态库 -> {} (pool={})", state.getUrl(), state.getPoolSize());
        return new HikariDataSource(hikari);
    }

    /** 状态库（Postgres）的数据库访问：运行态/审计保险箱/配额/日志都走 JdbcTemplate，统一从这里拿 */
    @Bean
    public JdbcTemplate stateJdbc(@Qualifier("stateDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }

    /** 状态库事务管理器：事务包住「查键+插记录+锁行+登记幂等键」等跨语句操作 */
    @Bean
    public PlatformTransactionManager stateTxManager(@Qualifier("stateDataSource") DataSource ds) {
        return new DataSourceTransactionManager(ds);
    }

    /** 便于 TransactionTemplate 式的调用：事务包住多条 SQL */
    @Bean
    public TransactionTemplate stateTx(PlatformTransactionManager txManager) {
        return new TransactionTemplate(txManager);
    }

    /** Trino 连接池的 JdbcTemplate；流式查询路径（SSE）仍从 getDataSource 原生取 */
    @Bean
    public JdbcTemplate trinoJdbc(@Qualifier("trinoDataSource") DataSource ds) {
        return new JdbcTemplate(ds);
    }
}
