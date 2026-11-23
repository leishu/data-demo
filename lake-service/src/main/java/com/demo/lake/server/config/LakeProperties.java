package com.demo.lake.server.config;

import com.demo.lake.common.batch.LakeJobs;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private final Audit audit = new Audit();
    private final Metrics metrics = new Metrics();

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

        /** 全局同时在跑的作业数；超出的进入 QUEUED，由调度器按提交顺序放行 */
        private int maxConcurrentJobs = 2;
        /** 调度器扫描 QUEUED 的间隔 */
        private int dispatchIntervalMs = 1000;
        /** 提交方式：local（本机子进程，默认）| kubernetes（K8s Job） */
        private String executor = "local";
        /** Kubernetes Job 提交参数（只在 executor=kubernetes 时用到） */
        private final Kubernetes kubernetes = new Kubernetes();
        /** 运行状态存储：复用 infra 的 Postgres，服务本身不再持有 RUNNING 状态 */
        private final State state = new State();

        @Getter
        @Setter
        public static class Kubernetes {
            /** in-cluster 时用 <a href="https://kubernetes.default.svc">...</a>，本地调试可指到代理 */
            private String apiUrl = "https://kubernetes.default.svc";
            private String namespace = "default";
            private String image = "spark:3.5.1";
            private String serviceAccount = "default";
            /** 等待 Job 终态的最长时间（秒）；状态变化由 Watch API 推送，不再按间隔轮询 */
            private int timeoutSeconds = 1800;
        }

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
            /** 状态库里日志行的保留天数 */
            private int logRetentionDays = 7;
        }
    }

    /**
     * 合规用的查询原文保险箱：默认关闭。开启后原文经脱敏 + AES-GCM 加密单独存，
     * 普通审计日志里仍然只有指纹；没有密钥时启动失败（fail closed）。
     */
    @Getter
    @Setter
    public static class Audit {
        private boolean sqlCaptureEnabled = false;
        /** 写入前脱敏：字符串字面量掩码，日期/数字保留 */
        private boolean redact = true;
        private int retentionDays = 30;
        /** 当前 AES-256 密钥（base64，32 字节），从环境变量注入，不进配置文件 */
        private String key = null;
        /** 当前密钥的编号，写进每条原文的 key_id 列 */
        private String keyId = "v1";
        /**
         * 历史密钥环：{@code LAKE_AUDIT_KEYS_V1 / V2 ...} 或 yml 里的 lake.audit.keys.v1。
         * 轮换时把旧密钥留在这里，老原文才读得出来；留空则只用上面的单密钥（兼容旧配置）。
         */
        private Map<String, String> keys = new HashMap<>();
    }

    /**
     * demo 级鉴权：静态账号 + HTTP Basic，无会话、无外部依赖。
     *
     * <p>生产环境请关掉这里的 enabled，改由前面的网关/IdP 做认证和授权；
     * 明文口令只适合本机演示（{noop} 前缀表示没有哈希）。</p>
     *
    ** 从 Iceberg 元数据表采集的表级指标（文件数 / 平均文件大小 / 快照数） */
    @Getter
    @Setter
    public static class Metrics {
        private final Iceberg iceberg = new Iceberg();

        @Getter
        @Setter
        public static class Iceberg {
            private boolean enabled = true;
            private int intervalSeconds = 60;
            /** 要采集的表名；默认三层业务表，可用 lake.metrics.iceberg.tables 覆盖 */
            private List<String> tables = List.of(
                    com.demo.lake.common.catalog.LakeTables.BRONZE_ORDERS,
                    com.demo.lake.common.catalog.LakeTables.SILVER_ORDERS,
                    com.demo.lake.common.catalog.LakeTables.GOLD_DAILY_REVENUE);
        }
    }

    @Getter
    @Setter
    public static class Security {
        private boolean enabled = true;
        private String realm = "lakehouse";
        /**
         * local=静态账号（demo 默认）；external=信任前置网关注入的身份头。
         * external 模式下必须配 shared-secret，否则所有请求都被拒绝（fail closed）。
         */
        private String mode = "local";
        /** external 模式下网关与服务的共享密钥，从 LAKE_SECURITY_SHARED_SECRET 注入，不进配置文件 */
        private String sharedSecret = null;
        /** viewer 只读；operator 额外可以触发批作业、看 actuator 指标 */
        private List<Account> accounts = List.of(
                new Account("viewer", "{noop}viewer", List.of("QUERY")),
                new Account("operator", "{noop}operator", List.of("QUERY", "BATCH")),
                new Account("auditor", "{noop}auditor", List.of("AUDITOR")));

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
