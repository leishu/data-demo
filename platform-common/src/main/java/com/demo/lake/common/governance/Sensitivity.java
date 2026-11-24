package com.demo.lake.common.governance;

import lombok.Getter;
import lombok.experimental.Accessors;

/**
 * 数据敏感级别：L1 公开 → L4 受限。
 *
 * <p>级别只在这里定义，资产目录引用枚举、页面按 {@link #level()} 渲染徽标颜色，
 * 不在前端另抄一份“敏感/机密”的字符串表。</p>
 */
@Getter
@Accessors(fluent = true)
public enum Sensitivity {

    /** L1：可对内公开 */
    PUBLIC("公开", 1),
    /** L2：仅内部使用 */
    INTERNAL("内部", 2),
    /** L3：含订单/金额等敏感字段 */
    SENSITIVE("敏感", 3),
    /** L4：高敏感，需最小范围访问 */
    RESTRICTED("受限", 4);

    private final String label;
    private final int level;

    Sensitivity(String label, int level) {
        this.label = label;
        this.level = level;
    }
}
