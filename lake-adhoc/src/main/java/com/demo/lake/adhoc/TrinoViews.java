package com.demo.lake.adhoc;

import com.demo.lake.common.catalog.LakeViews;
import com.demo.lake.common.cli.Args;
import com.demo.lake.common.config.PlatformConfig;
import com.demo.lake.common.sql.SqlScript;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

/**
 * 在 Trino 侧创建/刷新脱敏视图，给不启动 lake-service 的纯 Trino 环境用。
 *
 * <p>SQL 来自 platform-common 的 {@code sql/view/}，和 lake-service 启动时的
 * {@code LakeViewDdl} 用的是同一份模板；{@code --print-only} 可以不连库先看渲染结果。</p>
 *
 * <p>视图定义由 {@code etl} / {@code admin} 这类有 OWNERSHIP + GRANT_SELECT 的账号执行；
 * 分析和服务账号只需要 SELECT / GRANT_SELECT，不需要 DDL 权限。</p>
 */
public final class TrinoViews {

    public static void main(String[] argv) {
        Args args = Args.parse(argv);
        PlatformConfig platformConfig = PlatformConfig.load(args.has("conf") ? Paths.get(args.get("conf")) : null);
        List<String> statements = LakeViews.all(platformConfig);

        if (args.flag("print-only")) {
            for (String sql : statements) {
                System.out.println(sql);
                System.out.println();
            }
            return;
        }
        if (args.flag("help")) {
            System.out.println("用法: bin/init-views.sh [--url <jdbc>] [--server http://host:port] [--user <user>] [--print-only]");
            System.out.println("  --conf <path>       指定 platform.properties，默认 conf/platform.properties");
            System.out.println("  --user <user>       有建视图权限的 Trino 用户（security profile 下用 etl / admin）");
            System.out.println("  --print-only        只打印渲染后的 CREATE OR REPLACE VIEW，不连库");
            return;
        }

        String url = resolveUrl(args, platformConfig);
        String user = args.get("user", platformConfig.trinoUser());
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("timezone", "UTC");
        properties.setProperty("source", platformConfig.trinoSource() + "-views");

        System.out.println("Trino: " + url + " (user=" + user + ", views=" + statements.size() + ")");
        try (Connection conn = DriverManager.getConnection(url, properties)) {
            for (String sql : statements) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(sql);
                    System.out.println("[ok] " + SqlScript.oneLine(sql, 100));
                } catch (SQLException e) {
                    System.err.println("[失败] " + SqlScript.oneLine(sql, 100) + " -> " + e.getMessage());
                    System.exit(1);
                }
            }
        } catch (SQLException e) {
            System.err.println("无法连接 Trino: " + url);
            System.err.println("  原因: " + e.getMessage());
            System.err.println("  提示: 先执行 bin/infra.sh up trino，或检查 --server / --url");
            System.exit(3);
        }
    }

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

    private TrinoViews() {
    }
}
