package com.bailizhang.lynxdb.core.utils;

import com.bailizhang.lynxdb.core.common.G;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.*;

public interface BufferUtils {
    static String getString(ByteBuffer buffer) {
        return G.I.toString(BufferUtils.getBytes(buffer));
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

    static byte[] toBytes(Object o) {
        switch (o) {
            case String str -> {
                return G.I.toBytes(str);
            }

            case Byte b -> {
                return new byte[]{b};
            }

            case Short sht -> {
                ByteBuffer buffer = ByteBuffer.allocate(SHORT_LENGTH);
                return buffer.putShort(sht).array();
            }

            case Integer i -> {
                return intByteBuffer(i).array();
            }

            case Long l -> {
                ByteBuffer buffer = ByteBuffer.allocate(LONG_LENGTH);
                return buffer.putLong(l).array();
            }

            case Character c -> {
                return G.I.toBytes(String.valueOf(c));
            }

            case Float f -> {
                ByteBuffer buffer = ByteBuffer.allocate(FLOAT_LENGTH);
                return buffer.putFloat(f).array();
            }

            case Double d -> {
                ByteBuffer buffer = ByteBuffer.allocate(DOUBLE_LENGTH);
                return buffer.putDouble(d).array();
            }

            default -> throw new IllegalStateException("Unsupported parameter type: " + o.getClass().getName());
        }
    }

    /* 判断ByteBuffer是否读结束（或写结束） */
    static boolean isOver(ByteBuffer byteBuffer) {
        if(byteBuffer == null) {
            return false;
        }
        return byteBuffer.position() == byteBuffer.limit();
    }

    static boolean isOver(ByteBuffer[] buffers) {
        int len = buffers.length;
        if(len == 0) {
            throw new RuntimeException();
        }

        ByteBuffer lastBuffer = buffers[len-1];
        return lastBuffer.position() == lastBuffer.limit();
    }

    static boolean isNotOver(ByteBuffer byteBuffer) {
        return !isOver(byteBuffer);
    }

    static ByteBuffer byteByteBuffer(byte value) {
        return ByteBuffer.allocate(BYTE_LENGTH).put(value).rewind();
    }

    static ByteBuffer intByteBuffer() {
        return ByteBuffer.allocate(INT_LENGTH);
    }

    static ByteBuffer intByteBuffer(int value) {
        return ByteBuffer.allocate(INT_LENGTH).putInt(value).rewind();
    }

    static ByteBuffer longByteBuffer(long value) {
        return ByteBuffer.allocate(LONG_LENGTH).putLong(value).rewind();
    }

    static void write(ByteBuffer buffer, int offset, ByteBuffer[] data) {

    }

    static ByteBuffer[] toBuffers(byte[] data) {
        return new ByteBuffer[]{ByteBuffer.wrap(data)};
    }
}
