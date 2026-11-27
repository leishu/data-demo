package com.demo.lake.common.cli;

import com.demo.lake.common.batch.LakeJobs;
import lombok.Getter;
import lombok.experimental.Accessors;

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
    /** 只暴露位置参数；values / flags 是被解析用的内部状态，不给外部改 */
    @Getter
    @Accessors(fluent = true)
    private final List<String> positional = new ArrayList<>();

    public static Args parse(String[] argv) {
        Args args = new Args();
        for (int i = 0; i < argv.length; i++) {
            String token = argv[i];
            if (!token.startsWith("--")) {
                args.positional.add(token);
                continue;
            }
            String key = token.substring(2);
            boolean hasValue = i + 1 < argv.length && !argv[i + 1].startsWith("--");
            if (hasValue) {
                args.values.put(key, argv[++i]);
            } else {
                args.flags.add(key);
            }
        }
        return args;
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
        String value = values.get(key);
        return value == null ? def : Integer.parseInt(value);
    }

    public boolean flag(String key) {
        return flags.contains(key) || "true".equalsIgnoreCase(values.get(key));
    }

    public String job() {
        return get("job", LakeJobs.ALL).toLowerCase(Locale.ROOT);
    }
}
