package com.demo.lake.server.batch.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

/**
 * 日志文件的增量游标：每轮只读上次之后新增的字节，按行切分后吐出。
 *
 * <p>两个刻意的实现细节：</p>
 * <ul>
 *   <li>按 <b>字节</b> 找换行再逐行解码。先 decode 整块再切行的话，UTF-8 中文正好跨在
 *       64KB 边界上就会变成一串乱码——换行是 ASCII，按字节切绝不会切开多字节字符。</li>
 *   <li>没等到换行的尾巴先留在 {@code carry} 里，等下一轮补齐再发。否则前端会先看到
 *       半行、再看到完整的同一行。</li>
 * </ul>
 *
 * <p>非线程安全：一条 SSE 连接一个实例。</p>
 */
final class LogTailCursor {

    private static final int MAX_CHUNK_BYTES = 64 * 1024;
    /** 首帧预热只读文件末尾这么多字节：凑不够行数就算了，再往前读会拖慢首屏 */
    private static final int TAIL_CHUNK_BYTES = 256 * 1024;
    private static final int MAX_LINE_CHARS = 2000;
    private static final int MAX_BACKLOG = 2000;

    private final Deque<String> backlog = new ArrayDeque<>();
    private long offset;
    private byte[] carry = new byte[0];

    /**
     * 打开连接时的预热：只留文件末尾 maxLines 行，offset 直接停在文件尾，
     * 避免已经有几 MB 的 spark 日志被从头回放一遍。
     *
     * <p>块首被截断的半行直接丢弃；块尾未换行的尾巴留在 carry，等下一次 poll 补全。</p>
     */
    void primeTail(Path file, int maxLines) throws IOException {
        if (maxLines <= 0 || !Files.isReadable(file)) {
            return;
        }
        try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size == 0) {
                return;
            }
            int want = (int) Math.min(TAIL_CHUNK_BYTES, size);
            long from = size - want;
            channel.position(from);
            ByteBuffer buffer = ByteBuffer.allocate(want);
            while (buffer.hasRemaining() && channel.read(buffer) > 0) {
                // 读满尾部块
            }
            byte[] chunk = Arrays.copyOf(buffer.array(), buffer.position());
            offset = from + chunk.length;
            carry = new byte[0];

            int start = 0;
            if (from > 0) {
                // 从文件中间开始，第一行大概率是被截断的，从下一个换行之后算起
                int newline = indexOfNewline(chunk);
                if (newline < 0) {
                    // 整块只是半行：先攒着，下一轮和新增内容拼成完整行
                    carry = chunk;
                    return;
                }
                start = newline + 1;
            }
            keepTailLines(chunk, start, maxLines);
        }
    }

    /**
     * 读文件最后 maxLines 行（一次性快照）。
     *
     * <p>复用预热逻辑：只从文件末尾读一块，不把整个 spark 日志 {@code readAllLines} 进内存。</p>
     */
    static List<String> readTail(Path file, int maxLines) throws IOException {
        if (maxLines <= 0 || !Files.isReadable(file)) {
            return List.of();
        }
        LogTailCursor cursor = new LogTailCursor();
        cursor.primeTail(file, maxLines);
        return cursor.poll(file, maxLines);
    }

    private static int indexOfNewline(byte[] chunk) {
        for (int i = 0; i < chunk.length; i++) {
            if (chunk[i] == '\n') {
                return i;
            }
        }
        return -1;
    }

    /** 从 start 开始按行切，deque 只留最后 maxLines 行；末尾未换行的半行留在 carry */
    private void keepTailLines(byte[] chunk, int start, int maxLines) {
        int lineStart = start;
        for (int i = start; i < chunk.length; i++) {
            if (chunk[i] != '\n') {
                continue;
            }
            int length = i - lineStart;
            if (length > 0 && chunk[i - 1] == '\r') {
                length--;
            }
            backlog.addLast(truncate(new String(chunk, lineStart, length, StandardCharsets.UTF_8)));
            while (backlog.size() > maxLines) {
                backlog.pollFirst();
            }
            lineStart = i + 1;
        }
        carry = Arrays.copyOfRange(chunk, lineStart, chunk.length);
        if (carry.length > MAX_LINE_CHARS * 4) {
            backlog.addLast(truncate(new String(carry, StandardCharsets.UTF_8)));
            while (backlog.size() > maxLines) {
                backlog.pollFirst();
            }
            carry = new byte[0];
        }
    }

    /**
     * 增量读取：只吐上次之后新增的行（按字节找换行，避免多字节字符被切坏）。
     *
     * @param maxLines 本次最多返回多少行；一次读进来的行更多时，余下的留在 backlog，下次接着吐
     */
    List<String> poll(Path file, int maxLines) throws IOException {
        if (!Files.isReadable(file)) {
            return drain(maxLines);
        }
        try (SeekableByteChannel channel = Files.newByteChannel(file, StandardOpenOption.READ)) {
            long size = channel.size();
            if (size < offset) {
                // 文件被截断或按天轮转：从头再来，别拿着旧 offset 读越界
                offset = 0;
                carry = new byte[0];
                backlog.clear();
            }
            int want = (int) Math.min(MAX_CHUNK_BYTES, size - offset);
            if (want > 0) {
                channel.position(offset);
                ByteBuffer buffer = ByteBuffer.allocate(want);
                while (buffer.hasRemaining() && channel.read(buffer) > 0) {
                    // 读满一个 chunk（或读到当前文件尾）
                }
                byte[] chunk = Arrays.copyOf(buffer.array(), buffer.position());
                offset += chunk.length;
                split(chunk);
            }
        }
        return drain(maxLines);
    }

    private void split(byte[] chunk) {
        byte[] all = new byte[carry.length + chunk.length];
        System.arraycopy(carry, 0, all, 0, carry.length);
        System.arraycopy(chunk, 0, all, carry.length, chunk.length);

        int start = 0;
        for (int i = 0; i < all.length; i++) {
            if (all[i] != '\n') {
                continue;
            }
            int length = i - start;
            if (length > 0 && all[i - 1] == '\r') {
                length--;
            }
            backlog.addLast(truncate(new String(all, start, length, StandardCharsets.UTF_8)));
            start = i + 1;
        }
        carry = Arrays.copyOfRange(all, start, all.length);
        if (carry.length > MAX_LINE_CHARS * 4) {
            // 单行过长（比如把整个异常栈打成一行）：先发出去，别让 carry 无限涨
            backlog.addLast(truncate(new String(carry, StandardCharsets.UTF_8)));
            carry = new byte[0];
        }
        while (backlog.size() > MAX_BACKLOG) {
            backlog.pollFirst();
        }
    }

    private List<String> drain(int maxLines) {
        if (backlog.isEmpty()) {
            return List.of();
        }
        List<String> out = new ArrayList<>(Math.clamp(maxLines, 1, backlog.size()));
        while (!backlog.isEmpty() && out.size() < maxLines) {
            out.add(backlog.pollFirst());
        }
        return out;
    }

    private static String truncate(String line) {
        return line.length() <= MAX_LINE_CHARS ? line : line.substring(0, MAX_LINE_CHARS) + "…";
    }
}
