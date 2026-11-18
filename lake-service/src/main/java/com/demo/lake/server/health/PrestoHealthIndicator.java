package com.demo.lake.server.health;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
// Boot 4 把 actuator 的 health 抽到了独立模块 spring-boot-health，包名从 actuator.health 改为 health.contributor
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

/**
 * 把 Presto 的连通性挂到 /actuator/health 上。
 *
 * <p>Presto 没起来时服务自己仍然能启动（连接池初始化不抛错），但健康检查必须如实报 DOWN——
 * 这样 K8s readiness / 监控才能区分「服务活着但依赖挂了」和「服务挂了」。</p>
 */
@Component("presto")
@Slf4j
@RequiredArgsConstructor
public class PrestoHealthIndicator implements HealthIndicator {

    private final DataSource dataSource;

    @Override
    public Health health() {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.setQueryTimeout(3);
            stmt.execute("select 1");
            return Health.up().build();
        } catch (Exception e) {
            log.debug("Presto 健康检查失败", e);
            return Health.down().withDetail("error", String.valueOf(e.getMessage())).build();
        }
    }
}
