package com.demo.lake.server.governance.model;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * 资产目录里的一行：治理档案（来自 GovernanceCatalog）+ 表清单事实（来自 information_schema）
 * + 湖表健康度（来自 Iceberg 元数据表指标）。
 */
public record GovernanceAsset(
        @Schema(description = "表名") String table,
        @Schema(description = "schema，未建表时为 null") String schema,
        @Schema(description = "information_schema 里的表类型，未建表时为 null") String type,
        @Schema(description = "表在 information_schema 中存在") boolean exists,
        @Schema(description = "在 GovernanceCatalog 中登记") boolean registered,
        @Schema(description = "责任人，未登记时为 null") String owner,
        @Schema(description = "业务域，未登记时为 null") String domain,
        @Schema(description = "ODS / DWD / ADS，未登记时为 null") String layer,
        @Schema(description = "敏感级别枚举名：PUBLIC / INTERNAL / SENSITIVE / RESTRICTED") String sensitivity,
        @Schema(description = "敏感级别中文标签") String sensitivityLabel,
        @Schema(description = "敏感级别数字 1~4，便于前端着色") int sensitivityLevel,
        @Schema(description = "保留天数") int retentionDays,
        @Schema(description = "新鲜度 SLA（小时）") int freshnessSlaHours,
        @Schema(description = "表的用途说明") String description,
        @Schema(description = "最近一次采集到的湖表指标；没有则为 null 值字段") AssetMetrics metrics,
        @Schema(description = "按最近快照提交时间算出的新鲜度") Freshness freshness) {

    /** 文件数/快照数等；字段为 null 表示没采集到（指标未开启或 Trino 未就绪） */
    public record AssetMetrics(Long fileCount, Double avgFileSizeBytes, Long totalSizeBytes,
                               Long snapshotCount, Long lastCommittedMs) {
        public static final AssetMetrics EMPTY =
                new AssetMetrics(null, null, null, null, null);
    }

    /**
     * 新鲜度：用 Iceberg 最近快照提交时间对比 SLA。
     *
     * <p>没快照/没采集到就是 UNKNOWN，不拿当前时间冒充更新时间；
     * 超 SLA 但在 2 倍以内是 WARN，超过 2 倍才是 BREACH。</p>
     */
    public record Freshness(String status, Long lastUpdatedMs, Double hoursSinceUpdate) {
        public static final String OK = "OK";
        public static final String WARN = "WARN";
        public static final String BREACH = "BREACH";
        public static final String UNKNOWN = "UNKNOWN";

        private static final long HOUR_MS = 3_600_000L;

        public static Freshness of(long lastCommittedMs, int slaHours, long nowMs) {
            if (lastCommittedMs <= 0) {
                return new Freshness(UNKNOWN, null, null);
            }
            long hours = Math.max(0, (nowMs - lastCommittedMs) / HOUR_MS);
            String status = hours <= slaHours ? OK : hours <= 2L * slaHours ? WARN : BREACH;
            return new Freshness(status, lastCommittedMs, (double) hours);
        }

        /** 没登记就没有 SLA 可比：只报「多久没更新」，不判定 OK/BREACH */
        public static Freshness unknown(long lastCommittedMs, long nowMs) {
            if (lastCommittedMs <= 0) {
                return new Freshness(UNKNOWN, null, null);
            }
            long hours = Math.max(0, (nowMs - lastCommittedMs) / HOUR_MS);
            return new Freshness(UNKNOWN, lastCommittedMs, (double) hours);
        }
    }
}
