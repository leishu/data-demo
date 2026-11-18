package com.demo.lake.servier.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * 服务层自有配置（lake.*）。
 *
 * <p>注意与 {@link com.demo.lake.common.config.PlatformConfig} 的分工：平台配置（catalog / warehouse /
 * Presto 地址）仍然只有 platform-common 那一份来源，这里只放「服务怎么跑」的参数，避免两套配置打架。</p>
 *
 * <p>访问器由 Lombok 生成（JavaBean 风格，{@code getXxx}/{@code setXxx}）：Spring Boot 的
 * 宽松绑定依赖 setter，所以这两个注解是配置生效的必要条件，不能换成 {@code @Value} 或构造器绑定。</p>
 */
@ConfigurationProperties(prefix = "lake")
@Getter
@Setter
public class LakeProperties {

    /** 平台配置文件，相对路径按工作目录解析；不存在时退化为 classpath 默认值 + 环境变量 */
    private String confFile = "conf/platform.properties";

    // final：只允许改内层字段，不允许整体替换掉已绑定的对象
    private final Query query = new Query();
    private final Batch batch = new Batch();

    @Getter
    @Setter
    public static class Query {
        /** 单次返回行数上限，超出截断并标记 truncated */
        private int maxRows = 1000;
        /** 服务端兜底的查询超时（秒） */
        private int timeoutSeconds = 60;
        /** Hikari 连接池大小 */
        private int poolSize = 8;
        private int connectionTimeoutMs = 5000;
        /** 是否允许执行 DDL/DML —— 即席查询场景默认只读 */
        private boolean allowWrites = false;
    }

    @Getter
    @Setter
    public static class Batch {
        /** 关掉后 /api/batch/** 一律 403，适合只读环境 */
        private boolean enabled = true;
        /** {job} 会被替换成作业名；生产可换成 spark-submit --class ... 的完整命令 */
        private String commandTemplate = "bin/dev-batch.sh --job {job}";
        /** 子进程工作目录 */
        private String workDir = ".";
        /** 作业日志落盘目录 */
        private String logDir = "logs/batch";
        /** 单次作业超时（秒），超时后 destroy 子进程 */
        private int timeoutSeconds = 1800;
        /** 允许触发的作业名白名单：命令是拼接出来的，必须卡死来源 */
        private List<String> allowedJobs = List.of(
                "init", "ingest", "transform", "evolve", "maintain", "show", "verify-sql", "all");
    }
}
