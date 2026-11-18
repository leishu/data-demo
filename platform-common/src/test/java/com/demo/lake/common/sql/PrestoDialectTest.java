package com.demo.lake.common.sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrestoDialectTest {

    @Test
    void rewritesNegativeDateAddToDateSub() {
        assertEquals(
                "SELECT * FROM t WHERE dt >= date_sub(current_date, 6)",
                PrestoDialect.toSpark("SELECT * FROM t WHERE dt >= date_add('day', -6, current_date)"));
    }

    @Test
    void keepsPositiveDateAdd() {
        assertEquals(
                "SELECT date_add(current_date(), 3)",
                PrestoDialect.toSpark("SELECT date_add('day', 3, current_date())"));
    }

    @Test
    void rewritesQuotedInterval() {
        assertEquals(
                "SELECT current_date - INTERVAL 1 DAYS",
                PrestoDialect.toSpark("SELECT current_date - INTERVAL '1' DAY"));
    }

    @Test
    void leavesUnrelatedSqlUntouched() {
        String sql = "SELECT round(sum(gmv) / count(*), 2) FROM iceberg.lake.gold_daily_revenue";
        assertEquals(sql, PrestoDialect.toSpark(sql));
    }
}
