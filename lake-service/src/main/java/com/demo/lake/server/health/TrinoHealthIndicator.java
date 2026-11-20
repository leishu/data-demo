package com.demo.lake.server.health;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
// Boot 4 把 actuator 的 health 抽到了独立模块 spring-boot-health，包名从 actuator.health 改为 health.contributor
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/**
 * 把 Trino 的连通性挂到 /actuator/health 上。
 *
 * <p>Trino 没起来时服务自己仍然能启动（连接池初始化不抛错），但健康检查必须如实报 DOWN——
 * 这样 K8s readiness / 监控才能区分「服务活着但依赖挂了」和「服务挂了」。</p>
 *
 * <p>用 {@link ReactiveHealthIndicator} 而不是同步版：健康检查跑在 WebFlux 的事件循环上，
 * Trino 挂掉时 {@code getConnection()} 最长会阻塞 connectionTimeout（默认 5s），
 * 同步实现会把事件循环堵住，违背本项目「阻塞 IO 一律 offload」的约定。</p>
 */
@Component("trino")
@Slf4j
@RequiredArgsConstructor
public class TrinoHealthIndicator implements ReactiveHealthIndicator {

    @Qualifier("trinoDataSource")
    private final DataSource dataSource;
    private final Scheduler blocking;

    @Override
    public Mono<Health> health() {
        // 阻塞 JDBC 交给虚拟线程调度器，事件循环只负责订阅
        return Mono.fromCallable(this::check).subscribeOn(blocking);
    }

    private Health check() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(3);
            stmt.execute("select 1");
            return Health.up().build();
        } catch (Exception e) {
            log.debug("Trino 健康检查失败", e);
            return Health.down().withDetail("error", String.valueOf(e.getMessage())).build();
        }
    }
}
