package com.demo.lake.server.batch.model;

import com.demo.lake.common.batch.JobMeta;

import java.util.List;

/**
 * {@code /api/config} 里的作业卡片：字段名就是前端读取的字段名。
 *
 * <p>不直接把 {@link JobMeta} 交给 Jackson：虽然它已是 record，但把序列化规则散到
 * 公共模型里不值当；这里是 Web 层契约，字段名跟着前端走。</p>
 */
public record JobView(String name, String title, String desc, String group,
                      boolean cliOnly, boolean featured) {

    public static JobView of(JobMeta meta) {
        return new JobView(meta.name(), meta.title(), meta.desc(), meta.group(),
                meta.cliOnly(), meta.featured());
    }

    public static List<JobView> of(List<JobMeta> metas) {
        return metas.stream().map(JobView::of).toList();
    }
}
