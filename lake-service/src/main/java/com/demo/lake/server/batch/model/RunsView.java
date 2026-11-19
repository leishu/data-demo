package com.demo.lake.server.batch.model;

import java.util.List;

/**
 * {@code GET /api/batch/runs} 的响应：内存里的记录 ∪ 湖表 {@code ops.batch_runs} 的历史，
 * 按 id 去重、内存优先（内存里有 RUNNING、有真实退出码，比磁盘上的终态快照更准）。
 *
 * <p>列表里混了两份数据源，所以必须把「历史那份读没读到」告诉前端，
 * 让页面降级提示，而不是把整个列表接口打成 500：</p>
 * <ul>
 *   <li>{@link History#OK} 湖表可读，已合并</li>
 *   <li>{@link History#NOT_INITIALIZED} 表还没建（没跑过 init）</li>
 *   <li>{@link History#UNAVAILABLE} Trino 连不上</li>
 *   <li>{@link History#ERROR} 查询报错</li>
 * </ul>
 */
public record RunsView(List<BatchRun> runs, History history) {

    public record History(String status, String hint, String table) {

        public static final String OK = "OK";
        public static final String NOT_INITIALIZED = "NOT_INITIALIZED";
        public static final String UNAVAILABLE = "UNAVAILABLE";
        public static final String ERROR = "ERROR";
    }
}
