package com.demo.lake.common.governance;

import lombok.Getter;

/**
 * 一张湖表的治理档案：谁负责、属于哪个域/分层、敏感级别、保留多久、新鲜度 SLA 是多少。
 *
 * <p>这是配置数据，不是运行时状态：所有字段在构造时校验，目录写错在启动前的单测阶段就会挂掉，
 * 不会等到页面上出现“负责人为空”才被发现。</p>
 */
@Getter
public final class AssetDefinition {

    private final String table;
    private final String owner;
    private final String domain;
    private final String layer;
    private final Sensitivity sensitivity;
    private final int retentionDays;
    private final int freshnessSlaHours;
    private final String description;

    public AssetDefinition(String table, String owner, String domain, String layer,
                           Sensitivity sensitivity, int retentionDays, int freshnessSlaHours,
                           String description) {
        this.table = require(table, "表名");
        this.owner = require(owner, "负责人");
        this.domain = require(domain, "业务域");
        this.layer = require(layer, "分层");
        if (sensitivity == null) {
            throw new IllegalArgumentException("缺少必填配置: " + table + " 的敏感级别");
        }
        if (retentionDays <= 0) {
            throw new IllegalArgumentException("保留天数必须为正: " + table + " = " + retentionDays);
        }
        if (freshnessSlaHours <= 0) {
            throw new IllegalArgumentException("新鲜度 SLA 必须为正: " + table + " = " + freshnessSlaHours);
        }
        this.sensitivity = sensitivity;
        this.retentionDays = retentionDays;
        this.freshnessSlaHours = freshnessSlaHours;
        this.description = require(description, "描述");
    }

    private static String require(String value, String field) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("缺少必填配置: " + field);
        }
        return value.trim();
    }
}
