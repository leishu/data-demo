package com.demo.lake.common.catalog;

import com.demo.lake.common.config.PlatformConfig;

import java.util.Objects;

/** catalog.database.table 三段式标识，避免到处拼字符串 */
public final class TableRef {

    private final String catalog;
    private final String database;
    private final String table;

    private TableRef(String catalog, String database, String table) {
        this.catalog = catalog;
        this.database = database;
        this.table = table;
    }

    public static TableRef of(String catalog, String database, String table) {
        return new TableRef(catalog, database, table);
    }

    /** 使用平台默认 catalog/database */
    public static TableRef of(PlatformConfig cfg, String table) {
        return new TableRef(cfg.catalogName(), cfg.database(), table);
    }

    public String catalog() {
        return catalog;
    }

    public String database() {
        return database;
    }

    public String table() {
        return table;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableRef)) {
            return false;
        }
        TableRef that = (TableRef) o;
        return catalog.equals(that.catalog) && database.equals(that.database) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, database, table);
    }
}
