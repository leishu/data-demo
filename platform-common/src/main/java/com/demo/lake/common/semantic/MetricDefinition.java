package com.demo.lake.common.semantic;

import java.util.List;

/**
 * 一个受控的业务指标定义：指标名、口径表达式、来源表与允许的维度。
 *
 * <p>语义层的核心不是「再包一层 SQL」，而是把口径（GMV / 净收入 / 退款率）定义在一处，
 * 前端和其它调用方只能按定义组合维度，不能自己写任意聚合表达式。</p>
 */
public final class MetricDefinition {

    private final String name;
    private final String label;
    private final String description;
    private final String source;
    private final String measure;
    private final List<String> dimensions;

    public MetricDefinition(String name, String label, String description, String source,
                            String measure, List<String> dimensions) {
        this.name = name;
        this.label = label;
        this.description = description;
        this.source = source;
        this.measure = measure;
        this.dimensions = List.copyOf(dimensions);
    }

    public String getName() {
        return name;
    }

    public String getLabel() {
        return label;
    }

    public String getDescription() {
        return description;
    }

    public String getSource() {
        return source;
    }

    public String getMeasure() {
        return measure;
    }

    public List<String> getDimensions() {
        return dimensions;
    }
}
