package com.demo.lake.server.batch.service;

import com.demo.lake.server.batch.model.BatchRun;

/**
 * 作业提交方式。实现类用 bean 名注册（local / kubernetes），由 JobDispatcher 按配置挑选。
 *
 * <p>start 必须非阻塞：它只负责「把作业提交出去」并立刻返回外部句柄；
 * 真正的等待/轮询在实现里用虚拟线程做，终态通过 listener 回调。</p>
 */
public interface JobExecutor {

    /** @return 外部句柄（本地 PID / K8s Job 名），写进运行态便于排障 */
    String start(BatchRun run, JobOutcomeListener listener);

    /** 尽力取消；本地进程 SIGKILL，K8s Job 删除 */
    default void cancel(String handle) {
    }
}
