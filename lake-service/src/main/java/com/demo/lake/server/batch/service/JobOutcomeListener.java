package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;

/** 执行器跑完（或启动失败/超时）后的统一回调，解耦「怎么跑」和「状态怎么记」 */
@FunctionalInterface
public interface JobOutcomeListener {

    void completed(BatchRun run, String status, Integer exitCode, long elapsedMs, String message);
}
