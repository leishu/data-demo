package com.demo.lake.common.quality;

/**
 * 一个质量检查项：名字 / 中文名 / 级别 / 来源表 / SQL 模板资源。
 *
 * <p>与语义层、作业名一样是跨模块契约：作业侧按它执行 SQL，服务端按它解释结果，
 * 单测保证每个登记的模板都能渲染。</p>
 *
 * @param name     检查名（唯一）
 * @param label    中文展示名
 * @param severity 级别：ERROR 阻断作业，WARN 只记录
 * @param source   silver / gold：决定 snapshot_id 取哪张表的快照
 * @param resource classpath 下 sql/quality/ 的模板路径
 */
public record QualityCheck(String name, String label, QualitySeverity severity,
                           String source, String resource) {
}
