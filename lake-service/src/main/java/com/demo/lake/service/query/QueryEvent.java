package com.demo.lake.service.query;

import java.util.List;

/**
 * 查询 SSE 流的事件载荷。
 *
 * <p>{@link #type()} 既是 SSE 的 event 名，也随 JSON 一起下发：EventSource 只能按事件名监听，
 * 而用 fetch 手工解帧的客户端看字段判断，两种用法都能对上。</p>
 */
public sealed interface QueryEvent {

    String type();

    /** 列定义：连接一建立就先发，前端可以先画出表头 */
    record Meta(List<Column> columns) implements QueryEvent {
        @Override
        public String type() {
            return "meta";
        }
    }

    /** 一批数据行，会来多次；前端收到就追加渲染 */
    record Rows(List<List<String>> rows) implements QueryEvent {
        @Override
        public String type() {
            return "rows";
        }
    }

    /** 收尾：总行数、是否截断、耗时、QueryStats */
    record Done(int rowCount, boolean truncated, long elapsedMs, String stats) implements QueryEvent {
        @Override
        public String type() {
            return "done";
        }
    }

    /** 失败：SSE 的响应头在第一个事件发出时就已经 200 了，错误只能在流里报 */
    record Error(int status, String message, String hint) implements QueryEvent {
        @Override
        public String type() {
            return "error";
        }
    }
}
