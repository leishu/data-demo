package com.demo.lake.common.cli;

import com.demo.lake.common.sql.SqlScript;
import com.demo.lake.common.sql.SqlTemplate;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 即席查询 SQL 预处理器：把 {@code ${var}} 占位符替换成实际参数，缺参的语句整条跳过。
 *
 * <p>从 trino-adhoc 迁出，只保留「参数化」这一项自研约定；SQL 执行交给 lake-adhoc 里的官方 trino-jdbc。
 * 切句复用 {@link SqlScript#split(String)}，保证与 verify-sql 用同一套规则（跳过注释与字符串里的分号）。
 *
 * <p>用法（由 bin/trino-query.sh 调用，输出可直接喂给 Trino CLI）：
 * <pre>
 *   java ... com.demo.lake.common.cli.SqlParams &lt;sql-file&gt; \
 *     [--params k=v,k2=v2] [--explain]
 * </pre>
 */
public final class SqlParams {

    public static void main(String[] argv) {
        String file = null;
        Map<String, String> params = new LinkedHashMap<>();
        boolean explain = false;

        for (int i = 0; i < argv.length; i++) {
            String arg = argv[i];
            switch (arg) {
                case "--params":
                    if (i + 1 >= argv.length) {
                        fail("--params 缺少参数值");
                        return;
                    }
                    params.putAll(parseParams(argv[++i]));
                    break;
                case "--explain":
                    explain = true;
                    break;
                default:
                    if (file == null) {
                        file = arg;
                    } else {
                        fail("未知参数: " + arg);
                        return;
                    }
            }
        }

        if (file == null) {
            fail("用法: SqlParams <sql-file> [--params k=v,k2=v2] [--explain]");
            return;
        }

        String script = SqlScript.read(Paths.get(file));
        for (String sql : prepare(script, params, explain)) {
            System.out.println(sql);
            System.out.println();
        }
    }

    /**
     * 把脚本预处理成可直接执行的语句列表（供 lake-adhoc 复用，避免多起一次 JVM）：
     * 复用 {@link SqlScript#split(String)} 精确切句，替换 {@code ${var}}，缺参语句整条跳过。
     */
    public static List<String> prepare(String script, Map<String, String> params, boolean explain) {
        List<String> statements = new ArrayList<>();
        int skipped = 0;
        for (String stmt : SqlScript.split(script)) {
            String resolved = SqlTemplate.render(stmt, params);
            if (SqlTemplate.hasUnresolved(resolved)) {
                // 仍有未替换的占位符 = 缺参，整条跳过
                System.err.println("-- 跳过（缺少参数）: " + SqlScript.oneLine(stmt, 90));
                skipped++;
                continue;
            }
            String out = resolved.trim();
            if (explain && !out.toUpperCase().startsWith("EXPLAIN")) {
                out = "EXPLAIN " + out;
            }
            // SqlScript.split() 返回的语句已不含分号；JDBC 的 executeQuery 不接受末尾分号
            statements.add(out);
        }
        if (skipped > 0) {
            System.err.println("-- 共跳过 " + skipped + " 条缺参语句");
        }
        return statements;
    }

    /** 解析 {@code k=v,k2=v2}，取第一个等号切分，值可含等号。 */
    private static Map<String, String> parseParams(String raw) {
        Map<String, String> map = new LinkedHashMap<>();
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

    private static void fail(String message) {
        System.err.println(message);
        System.exit(2);
    }

    private SqlParams() {
    }
}
