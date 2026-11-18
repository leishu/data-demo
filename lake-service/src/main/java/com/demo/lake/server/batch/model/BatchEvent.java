package com.demo.lake.server.batch.model;

import java.util.List;

/** 作业监控 SSE 的事件；{@link #type()} 同时作为 SSE 的 event 名 */
public sealed interface BatchEvent {

    String type();

    /** 状态快照：提交、开始跑、结束都会发一次 */
    record Status(BatchRun run) implements BatchEvent {
        @Override
        public String type() {
            return "status";
        }
    }

    /** 日志增量（不是全量），前端 append 即可 */
    record Log(List<String> lines) implements BatchEvent {
        @Override
        public String type() {
            return "log";
        }
    }

    /** 终态：发完这条服务端就关连接 */
    record End(BatchRun run) implements BatchEvent {
        @Override
        public String type() {
            return "end";
        }
    }

    /** 心跳：作业长时间没输出时别让中间代理把空闲连接掐了，前端不监听这个事件即可 */
    record Ping() implements BatchEvent {
        @Override
        public String type() {
            return "ping";
        }
    }
}
