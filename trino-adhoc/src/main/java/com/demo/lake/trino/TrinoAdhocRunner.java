package com.demo.lake.trino;

import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlScript;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Trino 即席查询 CLI。
 *
 * <pre>
 * mvn -pl trino-adhoc exec:java -Dexec.args="--sql-file sql/adhoc/01_daily_revenue.sql"
 * mvn -pl trino-adhoc exec:java -Dexec.args="--sql \"select count(*) from iceberg.lake.silver_orders\""
 * mvn -pl trino-adhoc exec:java -Dexec.args="--list"
 * </pre>
 */
public final class TrinoAdhocRunner {

    private static final Path SCRIPT_DIR = Paths.get("sql", "adhoc");
    private static final Pattern VAR = Pattern.compile("\\$\\{([a-zA-Z0-9_.-]+)}");

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

        List<String> statements;
        String title;
        if (inlineSql != null) {
            statements = SqlScript.split(inlineSql);
            title = "inline";
        } else {
            Path path = Paths.get(file);
            if (!Files.isReadable(path)) {
                System.err.println("SQL 文件不存在: " + path.toAbsolutePath());
                System.exit(2);
                return;
            }
            statements = SqlScript.split(SqlScript.read(path));
            title = path.getFileName().toString();
        }

        Map<String, String> params = parseParams(args.get("params"));
        boolean printOnly = args.flag("print-only");
        boolean explain = args.flag("explain");
        int limit = args.getInt("limit", 200);
        boolean showStats = args.flag("stats");
        int timeout = args.getInt("timeout", 300);

        PlatformConfig cfg = PlatformConfig.load(args.has("conf") ? Paths.get(args.get("conf")) : null);
        String url = args.get("url", cfg.trinoJdbcUrl());
        String user = args.get("user", cfg.trinoUser());

        System.out.println("Trino: " + url + "  (user=" + user + ", script=" + title
                + ", statements=" + statements.size() + ")");

        List<String> runnable = new ArrayList<>();
        for (String stmt : statements) {
            String resolved = substitute(stmt, params);
            if (VAR.matcher(resolved).find()) {
                System.out.println("-- 跳过（缺少参数）: " + SqlScript.oneLine(stmt, 90));
                continue;
            }
            runnable.add(explain && !resolved.trim().toUpperCase().startsWith("EXPLAIN")
                    ? "EXPLAIN " + resolved : resolved);
        }

        if (printOnly) {
            for (int i = 0; i < runnable.size(); i++) {
                System.out.println("\n-- [" + (i + 1) + "]");
                System.out.println(runnable.get(i).trim() + ";");
            }
            return;
        }

        try (TrinoClient client = TrinoClient.connect(url, user, cfg.trinoSource(), timeout)) {
            int index = 0;
            for (String sql : runnable) {
                index++;
                System.out.println("\n--- [" + index + "/" + runnable.size() + "] "
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
                    if (args.flag("fail-fast")) {
                        System.exit(1);
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println("无法连接 Trino: " + url);
            System.err.println("  原因: " + e.getMessage());
            System.err.println();
            System.err.println("排查建议:");
            System.err.println("  1) 启动 Trino + HMS + MinIO:  bin/infra.sh up trino");
            System.err.println("  2) 或先不装 Trino，用 Spark 校验同一份 SQL:");
            System.err.println("     mvn -pl spark-batch -am exec:java "
                    + "-Dexec.args=\"--job verify-sql --sql-file " + (file == null ? "sql/adhoc/01_daily_revenue.sql" : file) + "\"");
            System.exit(3);
        }
    }

    private static Map<String, String> parseParams(String raw) {
        Map<String, String> map = new HashMap<>();
        if (raw == null || raw.trim().isEmpty()) {
            return map;
        }
        for (String pair : raw.split(",")) {
            int idx = pair.indexOf('=');
            if (idx > 0) {
                map.put(pair.substring(0, idx).trim(), pair.substring(idx + 1).trim());
            }
        }
        return map;
    }

    private static String substitute(String sql, Map<String, String> params) {
        if (params.isEmpty()) {
            return sql;
        }
        Matcher m = VAR.matcher(sql);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String key = m.group(1);
            String value = params.get(key);
            m.appendReplacement(sb, Matcher.quoteReplacement(value == null ? m.group(0) : value));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private static void listScripts() {
        if (!Files.isDirectory(SCRIPT_DIR)) {
            System.out.println("未找到 " + SCRIPT_DIR.toAbsolutePath() + "（请在工作目录为项目根时执行）");
            return;
        }
        System.out.println("可用脚本（" + SCRIPT_DIR + "）：");
        try (Stream<Path> stream = Files.list(SCRIPT_DIR)) {
            List<Path> files = stream.filter(p -> p.toString().endsWith(".sql"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .collect(Collectors.toList());
            for (Path p : files) {
                String first = firstComment(p);
                System.out.printf("  %-28s %s%n", p.getFileName(), first);
            }
        } catch (IOException e) {
            System.err.println("列目录失败: " + e.getMessage());
        }
    }

    private static String firstComment(Path p) {
        try {
            return Files.readAllLines(p).stream()
                    .filter(l -> l.trim().startsWith("--"))
                    .map(l -> l.replaceFirst("^\\s*--\\s?", ""))
                    .findFirst().orElse("");
        } catch (IOException e) {
            return "";
        }
    }

    private static void printUsage() {
        System.out.println("Trino 即席查询 CLI");
        System.out.println();
        System.out.println("用法:");
        System.out.println("  --sql-file <path>    执行 SQL 文件（可含多条语句）");
        System.out.println("  --sql \"<sql>\"         直接执行一条 SQL");
        System.out.println("  --list               列出 sql/adhoc 下的脚本");
        System.out.println("  --url <jdbc url>     默认取自配置: jdbc:trino://host:port/catalog/schema");
        System.out.println("  --user <user>        默认 demo");
        System.out.println("  --limit <n>          最多显示行数，默认 200");
        System.out.println("  --timeout <sec>      查询超时，默认 300");
        System.out.println("  --params k1=v1,k2=v2 替换 SQL 里的 ${k1}");
        System.out.println("  --explain            自动加 EXPLAIN 前缀");
        System.out.println("  --print-only         只打印要执行的语句，不连服务");
        System.out.println("  --stats              打印 Trino QueryStats");
        System.out.println("  --fail-fast          某条语句失败即退出");
    }

    private TrinoAdhocRunner() {
    }
}
