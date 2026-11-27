package com.demo.lake.server.observability.service;

import com.demo.lake.common.catalog.LakeViews;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlScript;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * 启动时通过 Trino 建/刷新脱敏视图（{@code CREATE OR REPLACE VIEW}）。
 *
 * <p>视图定义走 platform-common 的 {@code sql/view/} 模板，和 DDL 一样是单一事实源；
 * 这里只负责执行。基础表还没建（没跑 init）或 Trino 没起时保留降级：服务照常启动，
 * 查询接口会按既有逻辑返回 503，视图等下次启动或手动执行 {@code bin/init-views.sh} 再建。</p>
 *
 * <p>不用 {@link com.demo.lake.server.query.service.QueryService}：那条路径有只读守卫，
 * 会把 CREATE VIEW 拦掉；这里是服务自己可信的内部 DDL，直接用 Trino 连接池。</p>
 */
@Component
@Slf4j
public class LakeViewDdl {

    private final JdbcTemplate jdbc;
    private final PlatformConfig platformConfig;

    // 容器里有两个 JdbcTemplate（stateJdbc / trinoJdbc）：视图 DDL 必须走 Trino 那个，
    // 所以不能用 Lombok 生成的构造器，这里显式标 @Qualifier。
    public LakeViewDdl(@Qualifier("trinoJdbc") JdbcTemplate jdbc, PlatformConfig platformConfig) {
        this.jdbc = jdbc;
        this.platformConfig = platformConfig;
    }

    @PostConstruct
    void createViews() {
        for (String sql : LakeViews.all(platformConfig)) {
            try {
                jdbc.execute(sql);
                log.info("脱敏视图已就绪: {}", SqlScript.oneLine(sql, 120));
            } catch (DataAccessException e) {
                log.warn("创建脱敏视图失败（表未建或 Trino 未就绪）: {}", e.getMessage());
            }
        }
    }
}
