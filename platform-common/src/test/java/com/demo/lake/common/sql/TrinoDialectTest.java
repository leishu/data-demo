package com.demo.lake.common.sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TrinoDialectTest {

    @Test
    void dateAddWithNegativeValueBecomesDateSub() {
        assertEquals(
                "SELECT * FROM t WHERE dt >= date_sub(current_date, 6)",
                TrinoDialect.toSpark("SELECT * FROM t WHERE dt >= date_add('day', -6, current_date)"));
    }

    @Test
    void dateAddWithPositiveValueStaysDateAdd() {
        assertEquals(
                "SELECT date_add(current_date(), 3)",
                TrinoDialect.toSpark("SELECT date_add('day', 3, current_date())"));
    }

    @Test
    void quotedIntervalBecomesSparkInterval() {
        assertEquals(
                "SELECT current_date - INTERVAL 1 DAYS",
                TrinoDialect.toSpark("SELECT current_date - INTERVAL '1' DAY"));
    }

    @Test
    void unrelatedSqlIsUntouched() {
        String sql = "SELECT count(*) FROM iceberg.lake.silver_orders WHERE dt = '2026-01-01'";
        assertEquals(sql, TrinoDialect.toSpark(sql));
    }
}
