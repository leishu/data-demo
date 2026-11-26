package com.demo.lake.batch;

import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.cli.Args;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 锁住 --job all 的作业顺序：新作业要么是链路的一环，要么明确只在显式 --job 时跑。 */
class BatchAppPlanTest {

    @Test
    void all按建表加工质量维护巡检顺序跑() {
        List<String> plan = BatchApp.resolvePlan(Args.parse(new String[]{"--job", LakeJobs.ALL}));
        assertEquals(List.of(LakeJobs.INIT, LakeJobs.INGEST, LakeJobs.TRANSFORM,
                LakeJobs.QUALITY, LakeJobs.MAINTAIN, LakeJobs.SHOW), plan);
    }

    @Test
    void all带evolve时在加工之后插入演进() {
        List<String> plan = BatchApp.resolvePlan(
                Args.parse(new String[]{"--job", LakeJobs.ALL, "--evolve"}));
        assertEquals(List.of(LakeJobs.INIT, LakeJobs.INGEST, LakeJobs.TRANSFORM, LakeJobs.EVOLVE,
                LakeJobs.QUALITY, LakeJobs.MAINTAIN, LakeJobs.SHOW), plan);
    }

    @Test
    void 单作业模式不展开成全链路() {
        assertEquals(List.of(LakeJobs.QUALITY),
                BatchApp.resolvePlan(Args.parse(new String[]{"--job", LakeJobs.QUALITY})));
        assertEquals(List.of(LakeJobs.RETENTION),
                BatchApp.resolvePlan(Args.parse(new String[]{"--job", LakeJobs.RETENTION})));
    }
}
