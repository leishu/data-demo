package com.demo.lake.adhoc;

import com.demo.lake.common.cli.Args;
import com.demo.lake.common.cli.SqlParams;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlScript;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 即席查询入口：一次 JVM 内完成「SQL 预处理（SqlParams）→ 官方 trino-jdbc 执行 → 表格输出」。
 *
 * <path>协议层完全交给官方 trino-jdbc，项目只保留 ${var} 参数化这一层业务约定，
 * 不重复实现客户端。配置（catalog/database/trino 地址）来自 platform-common 的 PlatformConfig。
 */
public final class TrinoQuery {

    private static final Path SCRIPT_DIR = Paths.get("sql", "adhoc");

    public static void main(String[] argv) {
        Args args = Args.parse(argv);

        if (args.flag("help")) {
            printUsage();
            return;
        }
        if (args.flag("list")) {
            listScripts();
            return;
        }

        String inlineSql = args.get("sql");
        String file = args.get("sql-file");
        if (file == null && inlineSql == null && !args.positional().isEmpty()) {
            file = args.positional().get(0);
        }
        if (file == null && inlineSql == null) {
            printUsage();
            listScripts();
            return;
        }

        String script;
        String title;
        if (inlineSql != null) {
            script = inlineSql;
            title = "inline";
        } else {
            Path path = Paths.get(file);
            if (!Files.isReadable(path)) {
                System.err.println("SQL 文件不存在: " + path.toAbsolutePath());
                System.exit(2);
                return;
            }
            script = SqlScript.read(path);
            title = path.getFileName().toString();
        }

        boolean explain = args.flag("explain");
        boolean printOnly = args.flag("print-only");
        boolean showStats = args.flag("stats");
        boolean failFast = args.flag("fail-fast");
        int limit = args.getInt("limit", 200);
        int timeout = args.getInt("timeout", 300);
        Map<String, String> params = SqlParams.parseParams(args.get("params"));

        // 预处理：SqlParams 复用 SqlScript.split() 精确切句，替换 ${var}，缺参跳过
        List<String> statements = SqlParams.prepare(script, params, explain);
        if (statements.isEmpty()) {
            System.err.println("[!] 预处理后没有可执行的 SQL（可能全部语句都缺参数）");
            System.exit(1);
            return;
        }

        if (printOnly) {
            for (int i = 0; i < statements.size(); i++) {
                System.out.println("\n-- [" + (i + 1) + "]");
                System.out.println(statements.get(i));
            }
            return;
        }

        PlatformConfig platformConfig = PlatformConfig.load(args.has("conf") ? Paths.get(args.get("conf")) : null);
        String url = resolveUrl(args, platformConfig);
        String user = args.get("user", platformConfig.trinoUser());

        System.out.println("Trino: " + url + " (user=" + user + ", script=" + title
                + ", statements=" + statements.size() + ")");

        try (TrinoClient client = TrinoClient.connect(url, user, platformConfig.trinoSource(), timeout)) {
            for (int i = 0; i < statements.size(); i++) {
                String sql = statements.get(i);
                System.out.println("\n--- [" + (i + 1) + "/" + statements.size() + "] "
                        + SqlScript.oneLine(sql, 120));
                try {
                    TrinoClient.QueryResult result = client.execute(sql, limit);
                    System.out.print(TableFormatter.render(result.headers(), result.rows()));
                    System.out.printf("%d 行 · %d ms%s%n", result.rows().size(), result.elapsedMs(),
                            result.truncated() ? " · 已截断到 limit=" + limit : "");
                    if (showStats && result.stats() != null) {
                        System.out.println("[query stats] " + result.stats());
                    }
                } catch (SQLException e) {
                    System.err.println("[失败] " + e.getMessage());
                    if (failFast) {
                        System.exit(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("无法连接 Trino: " + url);
            System.err.println("  原因: " + e.getMessage());
            System.err.println("  提示: 先执行 bin/infra.sh up trino，或检查 --server / conf/platform.properties");
            System.exit(3);
        }
    }

    /** URL 优先级：--url（完整 JDBC）> --server（http://host:port）> PlatformConfig.trinoJdbcUrl() */
    private static String resolveUrl(Args args, PlatformConfig platformConfig) {
        String explicit = args.get("url");
        if (explicit != null && !explicit.isBlank()) {
            return explicit;
        }
        String server = args.get("server");
        if (server != null && !server.isBlank()) {
            String base = server.replaceFirst("^https?://", "").replaceAll("/$", "");
            return "jdbc:trino://" + base + "/" + platformConfig.catalogName() + "/" + platformConfig.database();
        }
        return platformConfig.trinoJdbcUrl();
    }

    private static void listScripts() {
        if (!Files.isDirectory(SCRIPT_DIR)) {
            System.out.println("未找到 " + SCRIPT_DIR.toAbsolutePath() + "（请在工作目录为项目根时执行）");
            return;
        }
        System.out.println("可用脚本（" + SCRIPT_DIR + "）：");
        try (Stream<Path> stream = Files.list(SCRIPT_DIR)) {
            List<Path> files = stream.filter(path -> path.toString().endsWith(".sql"))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .collect(Collectors.toList());
            for (Path path : files) {
                System.out.printf("  %-28s %s%n", path.getFileName(), firstComment(path));
            }
        } catch (Exception e) {
            System.err.println("列目录失败: " + e.getMessage());
        }
    }

    private static String firstComment(Path path) {
        try {
            return Files.readAllLines(path).stream()
                    .filter(l -> l.trim().startsWith("--"))
                    .map(l -> l.replaceFirst("^\\s*--\\s?", ""))
                    .findFirst().orElse("");
        } catch (Exception e) {
            return "";
        }
    }

    private static void printUsage() {
        System.out.println("即席查询（SqlParams 预处理 + 官方 trino-jdbc 执行）");
        System.out.println();
        System.out.println("用法: bin/trino-query.sh [选项]");
        System.out.println("  --sql-file <path>    执行 SQL 文件（可含多条语句、${var} 占位符）");
        System.out.println("  --sql \"<sql>\"         直接执行一条 SQL");
        System.out.println("  --list               列出 sql/adhoc 下的脚本");
        System.out.println("  --params k1=v1,k2=v2 替换 SQL 里的 ${k1}（缺参的语句自动跳过）");
        System.out.println("  --explain            自动给语句加 EXPLAIN 前缀");
        System.out.println("  --print-only         只打印预处理后的 SQL，不连服务");
        System.out.println("  --server <url>       http://host:port，默认取自 PlatformConfig");
        System.out.println("  --url <jdbc url>     完整 JDBC URL（优先级最高）");
        System.out.println("  --user <user>        默认取自 PlatformConfig");
        System.out.println("  --limit <n>          最多显示行数，默认 200");
        System.out.println("  --timeout <sec>      查询超时，默认 300");
        System.out.println("  --stats              打印 Trino QueryStats");
        System.out.println("  --fail-fast          某条语句失败即退出");
    }

    private TrinoQuery() {
    }
}
