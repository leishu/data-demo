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
    private static final int MAX_LINE_CHARS = 2000;
    private static final int MAX_BACKLOG = 2000;

    private final Deque<String> backlog = new ArrayDeque<>();
    private long offset;
    private byte[] carry = new byte[0];

    /**
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
        List<String> out = new ArrayList<>(Math.min(Math.max(maxLines, 1), backlog.size()));
        while (!backlog.isEmpty() && out.size() < maxLines) {
            out.add(backlog.pollFirst());
        }
        return out;
    }

    private static String truncate(String line) {
        return line.length() <= MAX_LINE_CHARS ? line : line.substring(0, MAX_LINE_CHARS) + "…";
    }
}
