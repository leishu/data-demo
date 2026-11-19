package com.demo.lake.common.batch;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LakeJobsTest {

    @Test
    void 元数据覆盖全部可选作业且顺序一致() {
        List<String> metaNames = LakeJobs.METAS.stream().map(JobMeta::name).collect(Collectors.toList());
        assertEquals(LakeJobs.CHOICES, metaNames);
    }

    @Test
    void 每个作业都有标题和描述() {
        for (JobMeta meta : LakeJobs.METAS) {
            assertFalse(meta.title().isBlank(), meta.name() + " 缺标题");
            assertFalse(meta.desc().isBlank(), meta.name() + " 缺描述");
            assertTrue(JobMeta.PIPELINE.equals(meta.group()) || JobMeta.OTHER.equals(meta.group()),
                    meta.name() + " 分组非法: " + meta.group());
        }
    }

    /** 页面卡片只由白名单决定，顺序按 METAS，不跟配置里写的顺序走 */
    @Test
    void 白名单决定可见卡片() {
        List<String> visible = LakeJobs.visible(List.of(LakeJobs.SHOW, LakeJobs.INIT, LakeJobs.ALL))
                .stream().map(JobMeta::name).collect(Collectors.toList());
        assertEquals(List.of(LakeJobs.INIT, LakeJobs.SHOW, LakeJobs.ALL), visible);
    }

    @Test
    void 空白名单与不认识的作业名都不出卡片() {
        assertTrue(LakeJobs.visible(null).isEmpty());
        assertTrue(LakeJobs.visible(List.of()).isEmpty());
        assertTrue(LakeJobs.visible(List.of("drop-everything")).isEmpty());
    }

    @Test
    void 展示开关与分组() {
        JobMeta verify = LakeJobs.visible(List.of(LakeJobs.VERIFY_SQL)).get(0);
        assertTrue(verify.cliOnly());
        assertFalse(verify.featured());
        assertEquals(JobMeta.OTHER, verify.group());

        JobMeta all = LakeJobs.visible(List.of(LakeJobs.ALL)).get(0);
        assertTrue(all.featured());
        assertFalse(all.cliOnly());

        JobMeta init = LakeJobs.visible(List.of(LakeJobs.INIT)).get(0);
        assertEquals(JobMeta.PIPELINE, init.group());
    }
}
