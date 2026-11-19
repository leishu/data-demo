package com.demo.lake.common.cli;

import com.demo.lake.common.batch.LakeJobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 极简命令行解析：{@code --key value} 或 {@code --flag}，外加位置参数。
 * 不引第三方 CLI 库，避免和 Spark 发行版里的 commons-cli 打架。
 */
public final class Args {

    private final Map<String, String> values = new HashMap<>();
    private final List<String> flags = new ArrayList<>();
    private final List<String> positional = new ArrayList<>();

    public static Args parse(String[] argv) {
        Args a = new Args();
        for (int i = 0; i < argv.length; i++) {
            String token = argv[i];
            if (!token.startsWith("--")) {
                a.positional.add(token);
                continue;
            }
            String key = token.substring(2);
            boolean hasValue = i + 1 < argv.length && !argv[i + 1].startsWith("--");
            if (hasValue) {
                a.values.put(key, argv[++i]);
            } else {
                a.flags.add(key);
            }
        }
        return a;
    }

    public boolean has(String key) {
        return values.containsKey(key) || flags.contains(key);
    }

    public String get(String key) {
        return values.get(key);
    }

    public String get(String key, String def) {
        return values.getOrDefault(key, def);
    }

    /** 入口解析出的结果回填给作业侧（如 run-id），保证同一进程内读到同一个值 */
    public void put(String key, String value) {
        values.put(key, value);
    }

    public int getInt(String key, int def) {
        String v = values.get(key);
        return v == null ? def : Integer.parseInt(v);
    }

    public boolean flag(String key) {
        return flags.contains(key) || "true".equalsIgnoreCase(values.get(key));
    }

    public List<String> positional() {
        return positional;
    }

    public String job() {
        return get("job", LakeJobs.ALL).toLowerCase(Locale.ROOT);
    }
}
