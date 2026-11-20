package com.demo.lake.server.controller;

import com.demo.lake.common.batch.JobMeta;
import com.demo.lake.common.batch.LakeJobs;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.server.batch.model.JobView;
import com.demo.lake.server.config.LakeProperties;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryControllerConfigTest {

    /** config() 只读配置，不碰 Trino：QueryService 传 null 即可 */
    private Map<String, Object> config(List<String> allowedJobs) {
        LakeProperties props = new LakeProperties();
        props.getBatch().setAllowedJobs(allowedJobs);
        return new QueryController(null, null, PlatformConfig.load(), props).config();
    }

    @Test
    @SuppressWarnings("unchecked")
    void 配置接口下发作业卡片() {
        Map<String, Object> batch = (Map<String, Object>) config(LakeJobs.CHOICES).get("batch");
        List<JobView> jobs = (List<JobView>) batch.get("jobs");

        assertEquals(LakeJobs.METAS.size(), jobs.size());
        JobView init = jobs.get(0);
        assertEquals(LakeJobs.INIT, init.name());
        assertEquals("建库建表", init.title());
        assertEquals(JobMeta.PIPELINE, init.group());
        assertFalse(init.cliOnly());
        assertEquals(LakeJobs.CHOICES, (List<String>) batch.get("allowedJobs"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void 白名单收窄后卡片同步减少() {
        Map<String, Object> batch = (Map<String, Object>) config(List.of(LakeJobs.INIT, LakeJobs.ALL)).get("batch");
        List<JobView> jobs = (List<JobView>) batch.get("jobs");

        assertEquals(List.of(LakeJobs.INIT, LakeJobs.ALL), jobs.stream().map(JobView::name).toList());
    }

    @Test
    @SuppressWarnings("unchecked")
    void cliOnly作业带标签下发() {
        Map<String, Object> batch = (Map<String, Object>) config(List.of(LakeJobs.VERIFY_SQL)).get("batch");
        List<JobView> jobs = (List<JobView>) batch.get("jobs");

        assertEquals(1, jobs.size());
        assertEquals(LakeJobs.VERIFY_SQL, jobs.get(0).name());
        assertTrue(jobs.get(0).cliOnly());
    }
}
