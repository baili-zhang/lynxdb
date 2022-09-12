package com.bailizhang.lynxdb.core.utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

public interface BufferUtils {
    byte[] EMPTY_BYTES = new byte[0];

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

    static byte[] toBytes(int src) {
        return intByteBuffer(src).array();
    }

    /* 判断ByteBuffer是否读结束（或写结束） */
    static boolean isOver(ByteBuffer byteBuffer) {
        if(byteBuffer == null) {
            return false;
        }
        return byteBuffer.position() == byteBuffer.limit();
    }

    static boolean isNotOver(ByteBuffer byteBuffer) {
        return !isOver(byteBuffer);
    }

    static ByteBuffer intByteBuffer() {
        return ByteBuffer.allocate(INT_LENGTH);
    }

    static ByteBuffer intByteBuffer(int value) {
        return ByteBuffer.allocate(INT_LENGTH).putInt(value).rewind();
    }

    static ByteBuffer longByteBuffer() {
        return ByteBuffer.allocate(LONG_LENGTH);
    }

    static ByteBuffer longByteBuffer(long value) {
        return ByteBuffer.allocate(LONG_LENGTH).putLong(value).rewind();
    }

    static List<String> toStringList(ByteBuffer buffer) {
        List<String> result = new ArrayList<>();

        while (!isOver(buffer)) {
            result.add(getString(buffer));
        }

        return result;
    }
}
