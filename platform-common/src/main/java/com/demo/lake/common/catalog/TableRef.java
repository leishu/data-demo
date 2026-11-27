package com.demo.lake.common.catalog;

import com.demo.lake.common.config.PlatformConfig;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.Accessors;

/** catalog.database.table 三段式标识，避免到处拼字符串 */
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class TableRef {

    private final String catalog;
    private final String database;
    private final String table;

    public static TableRef of(String catalog, String database, String table) {
        return new TableRef(catalog, database, table);
    }

    /** 使用平台默认 catalog/database */
    public static TableRef of(PlatformConfig platformConfig, String table) {
        return new TableRef(platformConfig.catalogName(), platformConfig.database(), table);
    }

    /** iceberg.lake.bronze_orders */
    public String fqn() {
        return catalog + "." + database + "." + table;
    }

    /** 元数据表：iceberg.lake.bronze_orders.snapshots */
    public String metadata(String name) {
        return fqn() + "." + name;
    }

    @Override
    public String toString() {
        return fqn();
    }
}
