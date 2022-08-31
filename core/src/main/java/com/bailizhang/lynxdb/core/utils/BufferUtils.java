package com.bailizhang.lynxdb.core.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bailizhang.lynxdb.core.utils.NumberUtils.INT_LENGTH;

public interface BufferUtils {
    static String getString(ByteBuffer buffer) {
        return new String(BufferUtils.getBytes(buffer));
    }

    static byte[] getBytes(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return bytes;
    }

    static byte[] getRemaining(ByteBuffer buffer) {
        int len = buffer.limit() - buffer.position();
        byte[] remaining = new byte[len];
        buffer.get(remaining);
        return remaining;
    }

    static String getRemainingString(ByteBuffer buffer) {
        byte[] bytes = getRemaining(buffer);
        return new String(bytes);
    }

    static byte[] toBytes(Collection<byte[]> src) {
        AtomicInteger length = new AtomicInteger(0);
        src.forEach(bytes -> length.getAndAdd(INT_LENGTH + bytes.length));
        ByteBuffer buffer = ByteBuffer.allocate(length.get());
        src.forEach(bytes -> buffer.putInt(bytes.length).put(bytes));
        return buffer.array();
    }

    /* 判断ByteBuffer是否读结束（或写结束） */
    static boolean isOver(ByteBuffer byteBuffer) {
        if(byteBuffer == null) {
            return false;
        }
        return byteBuffer.position() == byteBuffer.limit();
    }

    static ByteBuffer intByteBuffer() {
        return ByteBuffer.allocate(INT_LENGTH);
    }

    static List<String> toStringList(ByteBuffer buffer) {
        List<String> result = new ArrayList<>();

        while (!isOver(buffer)) {
            result.add(getString(buffer));
        }

        return result;
    }
}
