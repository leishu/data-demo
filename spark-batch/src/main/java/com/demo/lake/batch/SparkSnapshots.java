package com.demo.lake.batch;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * 从 Iceberg 元数据表读「最近一次提交」的快照 ID。
 *
 * <p>不能写 {@code max(snapshot_id)}：Iceberg 1.11 的 snapshot_id 由 UUID 生成，
 * 是随机正数而不是单调递增的序号，取 max 会随机命中历史快照（甚至已过期的那个）。
 * 唯一可靠的排序键是 {@code committed_at}，和 {@code ShowJob} 的快照巡检口径一致。</p>
 */
public final class SparkSnapshots {

    /** 包内可见给单测守卫：防止又改回 max(snapshot_id) 那种随机命中历史快照的写法 */
    static final String LATEST_SNAPSHOT =
            "SELECT snapshot_id FROM %s.snapshots ORDER BY committed_at DESC LIMIT 1";

    private SparkSnapshots() {
    }

    /** 表还没有快照时返回 null；元数据表读不到时抛 RuntimeException，由调用方按旁路纪律处理。 */
    public static String latestId(SparkSession spark, String fqn) {
        List<Row> rows = spark.sql(String.format(LATEST_SNAPSHOT, fqn)).collectAsList();
        if (rows.isEmpty()) {
            return null;
        }
        Object value = rows.get(0).get(0);
        return value == null ? null : String.valueOf(value);
    }
}
