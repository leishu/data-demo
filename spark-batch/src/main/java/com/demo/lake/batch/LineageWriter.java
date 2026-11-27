package com.demo.lake.batch;

import com.demo.lake.common.catalog.LakeTables;
import com.demo.lake.common.catalog.TableRef;
import com.demo.lake.common.lineage.LineageCatalog;
import com.demo.lake.common.config.PlatformConfig;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 每个写数据的作业跑完后落一行「run_id -> table -> snapshot_id」到 {@code ops.lineage_runs}；
 * init / maintain 这类只改结构或文件布局的作业不登记。
 *
 * <p>只靠 Iceberg 元数据表拿快照 ID，不改业务表结构。写入是旁路：拿不到快照或表不存在就跳过，
 * 记账失败也不改变作业结果（与 {@link RunRecordWriter} 同一纪律）。</p>
 */
final class LineageWriter {

    private LineageWriter() {
    }

    static void record(SparkSession spark, PlatformConfig platformConfig, String runId, String job) {
        List<String> tables = LineageCatalog.writtenTables(job);
        if (tables.isEmpty()) {
            return;
        }
        TableRef target = TableRef.of(platformConfig.catalogName(), LakeTables.OPS_SCHEMA, LakeTables.LINEAGE_RUNS);
        if (!spark.catalog().tableExists(target.fqn())) {
            return; // 老仓库还没跑过新 init：血缘表不存在，不阻塞作业
        }

        List<Row> rows = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (String table : tables) {
            String snapshotId;
            try {
                snapshotId = SparkSnapshots.latestId(spark, TableRef.of(platformConfig, table).fqn());
            } catch (RuntimeException e) {
                snapshotId = null; // 快照读不到就跳过，不影响作业结果
            }
            if (snapshotId == null) {
                continue; // 表刚建好还没写入，没有快照可记
            }
            rows.add(RowFactory.create(runId, job, table, snapshotId, now));
        }
        if (rows.isEmpty()) {
            return;
        }

        StructType schema = new StructType()
                .add("run_id", DataTypes.StringType, false)
                .add("job", DataTypes.StringType, false)
                .add("table_name", DataTypes.StringType, false)
                .add("snapshot_id", DataTypes.StringType, true)
                .add("recorded_ms", DataTypes.LongType, false);
        try {
            spark.createDataFrame(rows, schema).writeTo(target.fqn()).append();
            System.out.printf("[lineage] 已记录 %d 张表的快照（run_id=%s，job=%s）%n", rows.size(), runId, job);
        } catch (Exception e) {
            System.out.println("[lineage] 写入 " + target.fqn() + " 失败（不影响作业结果）: " + e.getMessage());
        }
    }
}
