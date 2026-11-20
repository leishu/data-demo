package com.demo.lake.server.controller;

import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.config.LakeProperties;
import com.demo.lake.server.query.InvalidSemanticQueryException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SemanticControllerTest {

    private SemanticController controller() {
        return new SemanticController(PlatformConfig.load(), new LakeProperties(), null, null, null, null);
    }

    @Test
    void 非法维度映射成语义请求异常() {
        SemanticController.SemanticRequest req = new SemanticController.SemanticRequest(
                "gmv", List.of("secret_col"), List.of(), 10, "trino");
        assertThrows(InvalidSemanticQueryException.class, () -> controller().render(req));
    }

    @Test
    void 同一口径渲染成两种方言() {
        SemanticController.SemanticRequest base = new SemanticController.SemanticRequest(
                "gmv", List.of("dt"), List.of(), 10, "trino");
        String trino = (String) controller().render(base).get("sql");
        String spark = (String) controller().render(new SemanticController.SemanticRequest(
                "gmv", List.of("dt"), List.of(), 10, "spark")).get("sql");

        assertTrue(trino.contains("\"dt\""), trino);
        assertTrue(spark.contains("`dt`"), spark);
        assertTrue(trino.contains("sum(gmv) as \"gmv\""));
        assertTrue(spark.contains("sum(gmv) as `gmv`"));
    }

    @Test
    void 指标目录非空() {
        assertTrue(controller().definitions().stream()
                .anyMatch(m -> "refund_rate".equals(m.getName())));
    }
}
