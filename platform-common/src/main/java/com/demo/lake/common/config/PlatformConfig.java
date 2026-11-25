package com.demo.lake.common.config;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/**
 * 平台统一配置。
 *
 * <p>优先级（后者覆盖前者）：</p>
 * <ol>
 *   <li>classpath:platform-defaults.properties</li>
 *   <li>工作目录下 conf/platform.properties（可选）</li>
 *   <li>调用方显式指定的 properties 文件</li>
 *   <li>环境变量 LAKE_XXX_YYY -&gt; platform.xxx.yyy（双下划线表示连字符，如 LAKE_S3_ACCESS__KEY）</li>
 *   <li>JVM 参数 -Dplatform.xxx.yyy</li>
 * </ol>
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public final class PlatformConfig {

    public static final String ENV_PREFIX = "LAKE_";
    private static final String DEFAULTS_RESOURCE = "platform-defaults.properties";
    private static final String DEFAULT_FILE = "conf/platform.properties";

    private final Properties props;

    public static PlatformConfig load() {
        return load(null);
    }

    public static PlatformConfig load(Path overrideFile) {
        Properties p = new Properties();
        loadClasspath(p, DEFAULTS_RESOURCE);

        Path defaultFile = Paths.get(DEFAULT_FILE);
        if (Files.isReadable(defaultFile)) {
            loadFile(p, defaultFile);
        }
        if (overrideFile != null) {
            if (!Files.isReadable(overrideFile)) {
                throw new IllegalArgumentException("配置文件不存在或不可读: " + overrideFile.toAbsolutePath());
            }
            loadFile(p, overrideFile);
        }
        applyEnvOverrides(p);
        applySystemOverrides(p);
        return new PlatformConfig(p);
    }

    // ------------------------------------------------------------------ 基础读取

    public String get(String key, String defaultValue) {
        String v = props.getProperty(key);
        return (v == null || v.trim().isEmpty()) ? defaultValue : v.trim();
    }

    public String getRequired(String key) {
        String v = props.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalStateException("缺少必填配置: " + key);
        }
        return v.trim();
    }

    public int getInt(String key, int defaultValue) {
        String v = props.getProperty(key);
        return (v == null || v.trim().isEmpty()) ? defaultValue : Integer.parseInt(v.trim());
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        String v = props.getProperty(key);
        return (v == null || v.trim().isEmpty()) ? defaultValue : Boolean.parseBoolean(v.trim());
    }

    // ------------------------------------------------------------------ Iceberg 目录

    public String catalogName() {
        return get("platform.catalog.name", "iceberg");
    }

    /** hadoop | hive */
    public String catalogType() {
        return get("platform.catalog.type", "hadoop").toLowerCase(Locale.ROOT);
    }

    public boolean isHiveCatalog() {
        return "hive".equals(catalogType());
    }

    public String database() {
        return get("platform.database", "lake");
    }

    /** 归一化后的仓库地址：本地相对路径 -&gt; file:///abs/path */
    public String warehouse() {
        String w = get("platform.warehouse", "warehouse");
        if (w.contains("://")) {
            return w.endsWith("/") ? w : w + "/";
        }
        String uri = Paths.get(w).toAbsolutePath().normalize().toUri().toString();
        return uri.endsWith("/") ? uri : uri + "/";
    }

    public String hiveUri() {
        return get("platform.hive.uri", "thrift://localhost:9083");
    }

    public boolean isObjectStorage() {
        String w = warehouse().toLowerCase(Locale.ROOT);
        return w.startsWith("s3a://") || w.startsWith("s3://");
    }

    public int formatVersion() {
        return getInt("platform.format-version", 2);
    }

    /** copy-on-write | merge-on-read —— 默认 cow，跨引擎读写最稳 */
    public String writeMode() {
        return get("platform.write-mode", "copy-on-write").toLowerCase(Locale.ROOT);
    }

    public String sparkMaster() {
        return get("platform.spark.master", "local[*]");
    }

    // ------------------------------------------------------------------ Spark

    /** 组装 SparkSession 需要的全部配置（catalog + 存储 + 执行参数） */
    public Map<String, String> sparkConf() {
        String c = catalogName();
        String prefix = "spark.sql.catalog." + c;
        Map<String, String> m = new LinkedHashMap<>();

        // Spark 4.x 默认开 ANSI（非法 cast / 除零直接抛异常）；保持 3.5 的宽容语义，
        // 避免既有 SQL 脚本和上游脏数据的行为漂移
        m.put("spark.sql.ansi.enabled", "false");
        // Spark 4.1 的 SPARK-59672：Iceberg SQL 扩展注入的 parser 会丢掉 ? 参数上下文，
        // 导致 spark.sql(sql, args) 报 UNBOUND_SQL_PARAMETER；打开 analyzer 侧的旧式替换兜底。
        // Iceberg 1.12 才从扩展侧修复，1.11 需要这个配置。
        m.put("spark.sql.legacy.parameterSubstitution.constantsOnly", "true");
        m.put("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        m.put(prefix, "org.apache.iceberg.spark.SparkCatalog");
        m.put(prefix + ".type", catalogType());
        m.put(prefix + ".warehouse", warehouse());
        if (isHiveCatalog()) {
            m.put(prefix + ".uri", hiveUri());
        }
        if (isObjectStorage()) {
            m.put(prefix + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            m.put(prefix + ".s3.endpoint", get("platform.s3.endpoint", ""));
            m.put(prefix + ".s3.path-style-access", String.valueOf(s3PathStyle()));
            m.put(prefix + ".s3.access-key-id", get("platform.s3.access-key", ""));
            m.put(prefix + ".s3.secret-access-key", get("platform.s3.secret-key", ""));
            // S3FileIO 的 region 走共享的 client.region；
            // 少了它，AWS SDK v2 会抛 "Unable to load region from any of the providers"
            m.put(prefix + ".client.region", get("platform.s3.region", "us-east-1"));

            m.put("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            m.put("spark.hadoop.fs.s3a.endpoint", get("platform.s3.endpoint", ""));
            m.put("spark.hadoop.fs.s3a.path.style.access", String.valueOf(s3PathStyle()));
            m.put("spark.hadoop.fs.s3a.access.key", get("platform.s3.access-key", ""));
            m.put("spark.hadoop.fs.s3a.secret.key", get("platform.s3.secret-key", ""));
            m.put("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            // Spark 4.1 起 s3a 默认启用 Magic Committer；数据文件走 Iceberg S3FileIO，
            // 显式关掉以免 s3a 路径上的写入语义相对 3.5 发生变化
            m.put("spark.hadoop.fs.s3a.committer.magic.enabled", "false");
        }

        m.put("spark.sql.session.timeZone", get("platform.spark.timezone", "UTC"));
        m.put("spark.sql.sources.partitionOverwriteMode", "dynamic");
        m.put("spark.sql.shuffle.partitions", get("platform.spark.shuffle.partitions", "8"));
        m.put("spark.sql.adaptive.enabled", "true");
        m.put("spark.sql.adaptive.coalescePartitions.enabled", "true");
        m.put("spark.sql.adaptive.skewJoin.enabled", "true");
        return m;
    }

    private boolean s3PathStyle() {
        return getBoolean("platform.s3.path-style-access", true);
    }

    // ------------------------------------------------------------------ Trino

    public String trinoHost() {
        return get("platform.trino.host", "localhost");
    }

    public int trinoPort() {
        return getInt("platform.trino.port", 8080);
    }

    public String trinoUser() {
        return get("platform.trino.user", "demo");
    }

    public String trinoSource() {
        return get("platform.trino.source", "lakehouse-adhoc");
    }

    /** jdbc:trino://host:port/catalog/schema */
    public String trinoJdbcUrl() {
        String explicit = get("platform.trino.url", null);
        if (explicit != null) {
            return explicit;
        }
        return String.format("jdbc:trino://%s:%d/%s/%s",
                trinoHost(), trinoPort(), catalogName(), database());
    }

    // ------------------------------------------------------------------ 可观测

    /** 打日志用，密钥打码 */
    public String describe() {
        String ak = get("platform.s3.access-key", "");
        String masked = ak.length() <= 2 ? "**" : ak.substring(0, 2) + "****";
        return String.format(
                "catalog=%s(type=%s) database=%s warehouse=%s formatVersion=%d writeMode=%s hiveUri=%s s3Auth=%s",
                catalogName(), catalogType(), database(), warehouse(), formatVersion(), writeMode(),
                isHiveCatalog() ? hiveUri() : "-", isObjectStorage() ? masked : "-");
    }

    // ------------------------------------------------------------------ 加载实现

    private static void loadClasspath(Properties p, String resource) {
        try (InputStream in = PlatformConfig.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("classpath 中找不到 " + resource);
            }
            try (Reader r = new InputStreamReader(in, StandardCharsets.UTF_8)) {
                p.load(r);
            }
        } catch (IOException e) {
            throw new IllegalStateException("读取 " + resource + " 失败", e);
        }
    }

    private static void loadFile(Properties p, Path file) {
        try (Reader r = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            p.load(r);
        } catch (IOException e) {
            throw new IllegalStateException("读取配置文件失败: " + file.toAbsolutePath(), e);
        }
    }

    /**
     * 环境变量名转配置 key（不是 LAKE_ 开头返回 null）：
     * LAKE_CATALOG_TYPE -&gt; platform.catalog.type
     * LAKE_S3_ACCESS__KEY -&gt; platform.s3.access-key（双下划线表示连字符）
     */
    static String envKeyToProperty(String envKey) {
        if (!envKey.startsWith(ENV_PREFIX) || envKey.length() <= ENV_PREFIX.length()) {
            return null;
        }
        String suffix = envKey.substring(ENV_PREFIX.length()).toLowerCase(Locale.ROOT)
                .replace("__", "\u0000")
                .replace('_', '.')
                .replace('\u0000', '-');
        return "platform." + suffix;
    }

    private static void applyEnvOverrides(Properties p) {
        System.getenv().forEach((k, v) -> {
            String key = envKeyToProperty(k);
            if (key != null) {
                p.setProperty(key, v);
            }
        });
    }

    private static void applySystemOverrides(Properties p) {
        for (String key : System.getProperties().stringPropertyNames()) {
            if (key.startsWith("platform.")) {
                p.setProperty(key, System.getProperty(key));
            }
        }
    }
}
