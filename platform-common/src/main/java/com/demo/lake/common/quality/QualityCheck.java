package com.demo.lake.common.quality;

import lombok.Getter;

/**
 * 一个质量检查项：名字 / 中文名 / 级别 / 来源表 / SQL 模板资源。
 *
 * <p>与语义层、作业名一样是跨模块契约：作业侧按它执行 SQL，服务端按它解释结果，
 * 单测保证每个登记的模板都能渲染。Java 11 不能用 record，用不可变类 + Lombok getter。</p>
 */
@Getter
public final class QualityCheck {

    private final String name;
    private final String label;
    private final QualitySeverity severity;
    /** silver / gold：决定 snapshot_id 取哪张表的快照 */
    private final String source;
    /** classpath 下 sql/quality/ 的模板路径 */
    private final String resource;

    public QualityCheck(String name, String label, QualitySeverity severity, String source, String resource) {
        this.name = name;
        this.label = label;
        this.severity = severity;
        this.source = source;
        this.resource = resource;
    }
}
