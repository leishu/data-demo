package com.demo.lake.common.sql;

import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 极简 SQL 脚本工具：读文件 + 按分号切分语句。
 * 只处理单引号字符串与 -- / 块注释，足够应付 ad-hoc 脚本与 DDL 文件。
 */
@UtilityClass
public final class SqlScript {

    public static String read(Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("读取 SQL 文件失败: " + path.toAbsolutePath(), e);
        }
    }

    public static String readResource(String resource) {
        try (InputStream in = SqlScript.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalArgumentException("classpath 中找不到 SQL 资源: " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("读取 SQL 资源失败: " + resource, e);
        }
    }

    /** 按分号切分，跳过字符串字面量与注释里的分号；返回的语句已去掉行尾分号 */
    public static List<String> split(String script) {
        List<String> out = new ArrayList<>();
        if (script == null || script.isEmpty()) {
            return out;
        }
        StringBuilder cur = new StringBuilder();
        boolean inSingle = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        for (int i = 0; i < script.length(); i++) {
            char ch = script.charAt(i);
            char next = i + 1 < script.length() ? script.charAt(i + 1) : '\0';

            if (inLineComment) {
                cur.append(ch);
                if (ch == '\n') {
                    inLineComment = false;
                }
                continue;
            }
            if (inBlockComment) {
                cur.append(ch);
                if (ch == '*' && next == '/') {
                    cur.append(next);
                    i++;
                    inBlockComment = false;
                }
                continue;
            }
            if (inSingle) {
                cur.append(ch);
                if (ch == '\'') {
                    inSingle = false;
                }
                continue;
            }

            if (ch == '-' && next == '-') {
                cur.append(ch).append(next);
                i++;
                inLineComment = true;
            } else if (ch == '/' && next == '*') {
                cur.append(ch).append(next);
                i++;
                inBlockComment = true;
            } else if (ch == '\'') {
                cur.append(ch);
                inSingle = true;
            } else if (ch == ';') {
                addIfMeaningful(out, cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        addIfMeaningful(out, cur.toString());
        return out;
    }

    private static void addIfMeaningful(List<String> out, String stmt) {
        String trimmed = stripComments(stmt).trim();
        if (!trimmed.isEmpty()) {
            out.add(stmt.trim());
        }
    }

    /** 去掉 -- 与块注释，用于判断语句是否为空、以及生成日志友好的单行摘要 */
    public static String stripComments(String sql) {
        StringBuilder sb = new StringBuilder(sql.length());
        boolean inSingle = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        for (int i = 0; i < sql.length(); i++) {
            char ch = sql.charAt(i);
            char next = i + 1 < sql.length() ? sql.charAt(i + 1) : '\0';
            if (inLineComment) {
                if (ch == '\n') {
                    inLineComment = false;
                    sb.append(ch);
                }
                continue;
            }
            if (inBlockComment) {
                if (ch == '*' && next == '/') {
                    inBlockComment = false;
                    i++;
                }
                continue;
            }
            if (inSingle) {
                sb.append(ch);
                if (ch == '\'') {
                    inSingle = false;
                }
                continue;
            }
            if (ch == '-' && next == '-') {
                inLineComment = true;
                i++;
            } else if (ch == '/' && next == '*') {
                inBlockComment = true;
                i++;
            } else if (ch == '\'') {
                inSingle = true;
                sb.append(ch);
            } else {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    /** 折叠空白，方便打一行日志 */
    public static String oneLine(String sql, int maxLen) {
        String s = stripComments(sql).replaceAll("\\s+", " ").trim();
        return s.length() <= maxLen ? s : s.substring(0, maxLen) + " ...";
    }
}
