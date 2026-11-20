package com.demo.lake.server.config;

import com.demo.lake.common.batch.LakeJobs;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

/**
 * 服务层自有配置（lake.*）。
 *
 * <p>注意与 {@link com.demo.lake.common.config.PlatformConfig} 的分工：平台配置（catalog / warehouse /
 * Trino 地址）仍然只有 platform-common 那一份来源，这里只放「服务怎么跑」的参数，避免两套配置打架。</p>
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
    private final Security security = new Security();

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
        /** 全局并发查询上限：Hikari 池只是连接数，这里限制「排队等连接」的请求数 */
        private int maxConcurrentQueries = 4;
        /** 单账号每分钟最多提交多少次查询（含 SSE 与表采样），滑动窗口 */
        private int maxQueriesPerMinute = 60;
        /** 并发名额最长等待时间，超时直接 429，不让请求一直挂在事件循环上 */
        private int acquireTimeoutMs = 3000;
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
        /**
         * 允许触发的作业名白名单：命令是拼接出来的，必须卡死来源。
         * 默认值取 common 的作业名表，新增作业时不用回来再抄一遍。
         */
        private List<String> allowedJobs = LakeJobs.CHOICES;

        /** 运行状态存储：复用 infra 的 Postgres，服务本身不再持有 RUNNING 状态 */
        private final State state = new State();

        @Getter
        @Setter
        public static class State {
            private String url = "jdbc:postgresql://localhost:5432/metastore";
            private String user = "hive";
            private String password = "hive";
            private int poolSize = 4;
            /** 心跳周期：更新本实例名下 RUNNING 记录的心跳时间 */
            private int heartbeatSeconds = 15;
            /** 超过这个时间没心跳的 RUNNING 记录视为实例失联，进入对账 */
            private int staleAfterSeconds = 120;
            /** 对账周期 */
            private int reconcileSeconds = 60;
        }
    }

    /**
     * demo 级鉴权：静态账号 + HTTP Basic，无会话、无外部依赖。
     *
     * <p>生产环境请关掉这里的 enabled，改由前面的网关/IdP 做认证和授权；
     * 明文口令只适合本机演示（{noop} 前缀表示没有哈希）。</p>
     */
    @Getter
    @Setter
    public static class Security {
        private boolean enabled = true;
        private String realm = "lakehouse";
        /** viewer 只读；operator 额外可以触发批作业、看 actuator 指标 */
        private List<Account> accounts = List.of(
                new Account("viewer", "{noop}viewer", List.of("QUERY")),
                new Account("operator", "{noop}operator", List.of("QUERY", "BATCH")));

        @Getter
        @Setter
        public static class Account {
            private String username;
            private String password;
            private List<String> roles = List.of();

            public Account() {
            }

            public Account(String username, String password, List<String> roles) {
                this.username = username;
                this.password = password;
                this.roles = roles;
            }
        }
    }
}
