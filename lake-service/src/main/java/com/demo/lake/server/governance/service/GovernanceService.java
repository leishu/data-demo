package com.demo.lake.server.governance.service;

import com.demo.lake.common.governance.AssetDefinition;
import com.demo.lake.common.governance.GovernanceCatalog;
import com.demo.lake.common.governance.Sensitivity;
import com.demo.lake.server.governance.model.GovernanceAsset;
import com.demo.lake.server.governance.model.GovernanceAsset.AssetMetrics;
import com.demo.lake.server.governance.model.GovernanceAsset.Freshness;
import com.demo.lake.server.observability.service.IcebergTableMetrics;
import com.demo.lake.server.observability.service.IcebergTableMetrics.TableSnapshot;
import com.demo.lake.server.query.model.TableInfo;
import com.demo.lake.server.query.service.QueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 资产目录装配：information_schema 的事实 + GovernanceCatalog 的档案 + Iceberg 指标的健康度。
 *
 * <p>三份数据各管各的：表是不是真的存在由 information_schema 说，责任人/级别/保留期由 common 的
 * 目录常量说，多久没更新由 Iceberg 快照说。服务只负责拼装，不维护第四份副本。</p>
 */
@Service
@RequiredArgsConstructor
public class GovernanceService {

    private final QueryService queries;
    private final IcebergTableMetrics iceberg;

    /** nowMs 由调用方传入，便于单测固定时钟；返回 Mono 是因为表清单要走 Trino 的阻塞 IO */
    public Mono<List<GovernanceAsset>> assets(long nowMs) {
        return queries.listTables().map(tables -> {
            Map<String, TableInfo> byName = tables.stream().collect(Collectors.toMap(
                    TableInfo::table, Function.identity(), (a, b) -> a, LinkedHashMap::new));
            Map<String, TableSnapshot> stats = iceberg.snapshot();

            List<GovernanceAsset> out = new ArrayList<>();
            // 目录顺序优先：页面清单稳定，且未建表的资产也要露出来（提醒还没 init）
            for (AssetDefinition def : GovernanceCatalog.ASSETS) {
                out.add(registered(def, byName.get(def.getTable()), stats.get(def.getTable()), nowMs));
            }
            // 反查：information_schema 里有、目录没登记的，也要显示成「未登记」而不是悄悄丢掉
            for (TableInfo info : byName.values()) {
                if (GovernanceCatalog.get(info.table()) == null) {
                    out.add(unregistered(info, stats.get(info.table()), nowMs));
                }
            }
            return List.copyOf(out);
        });
    }

    private static GovernanceAsset registered(AssetDefinition def, TableInfo info,
                                              TableSnapshot snapshot, long nowMs) {
        Sensitivity sensitivity = def.getSensitivity();
        long committed = snapshot == null ? 0 : snapshot.lastCommittedMs();
        return new GovernanceAsset(
                def.getTable(),
                info == null ? null : info.schema(),
                info == null ? null : info.type(),
                info != null,
                true,
                def.getOwner(),
                def.getDomain(),
                def.getLayer(),
                sensitivity.name(),
                sensitivity.label(),
                sensitivity.level(),
                def.getRetentionDays(),
                def.getFreshnessSlaHours(),
                def.getDescription(),
                metrics(snapshot),
                Freshness.of(committed, def.getFreshnessSlaHours(), nowMs));
    }

    private static GovernanceAsset unregistered(TableInfo info, TableSnapshot snapshot, long nowMs) {
        long committed = snapshot == null ? 0 : snapshot.lastCommittedMs();
        return new GovernanceAsset(
                info.table(), info.schema(), info.type(), true, false,
                null, null, null, null, "未登记", 0, 0, 0,
                "这张表在 information_schema 里，但 GovernanceCatalog 没有档案",
                metrics(snapshot),
                Freshness.unknown(committed, nowMs));
    }

    private static AssetMetrics metrics(TableSnapshot s) {
        if (s == null) {
            return AssetMetrics.EMPTY;
        }
        return new AssetMetrics(s.files(), s.avgFileSizeBytes(), s.totalSizeBytes(),
                s.snapshots(), s.lastCommittedMs() == 0 ? null : s.lastCommittedMs());
    }
}
