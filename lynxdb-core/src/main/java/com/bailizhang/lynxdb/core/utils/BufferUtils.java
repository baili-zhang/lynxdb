package com.bailizhang.lynxdb.core.utils;

import com.bailizhang.lynxdb.core.common.G;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.*;

public interface BufferUtils {
    byte[] EMPTY_BYTES = new byte[0];
    byte EMPTY_BYTE = (byte) 0x00;

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

    static byte[] toBytes(short src) {
        ByteBuffer buffer = ByteBuffer.allocate(SHORT_LENGTH);
        return buffer.putShort(src).array();
    }

    static byte[] toBytes(int src) {
        return intByteBuffer(src).array();
    }

    static byte[] toBytes(int[] src) {
        return toBuffer(src).array();
    }

    static byte[] toBytes(long src) {
        ByteBuffer buffer = ByteBuffer.allocate(LONG_LENGTH);
        return buffer.putLong(src).array();
    }

    static byte[] toBytes(float src) {
        ByteBuffer buffer = ByteBuffer.allocate(FLOAT_LENGTH);
        return buffer.putFloat(src).array();
    }

    static byte[] toBytes(double src) {
        ByteBuffer buffer = ByteBuffer.allocate(DOUBLE_LENGTH);
        return buffer.putDouble(src).array();
    }

    static byte[] toBytes(Object o) {
        Class<?> parameterType = o.getClass();

        if (ClassUtils.isString(parameterType)) {
            return G.I.toBytes((String) o);
        } else if (ClassUtils.isByte(parameterType)) {
            return new byte[]{(byte) o};
        } else if (ClassUtils.isShort(parameterType)) {
            return toBytes((short) o);
        } else if (ClassUtils.isInt(parameterType)) {
            return toBytes((int) o);
        } else if (ClassUtils.isLong(parameterType)) {
            return toBytes((long) o);
        } else if (ClassUtils.isChar(parameterType)) {
            return G.I.toBytes(String.valueOf((char) o));
        } else if (ClassUtils.isFloat(parameterType)) {
            return toBytes((float) o);
        } else if (ClassUtils.isDouble(parameterType)) {
            return toBytes((double) o);
        } else {
            throw new RuntimeException("Unsupported parameter type: " + parameterType.getName());
        }
    }

    static ByteBuffer toBuffer(int[] src) {
        int len = src.length * INT_LENGTH;
        ByteBuffer buffer = ByteBuffer.allocate(len);

        for(int i : src) {
            buffer.putInt(i);
        }

        return buffer.rewind();
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

    static Object getByType(ByteBuffer buffer, Class<?> parameterType) {
        if (ClassUtils.isString(parameterType)) {
            return getString(buffer);
        } else if (ClassUtils.isByte(parameterType)) {
            int len = buffer.getInt();
            assert len == BYTE_LENGTH;
            return buffer.get();
        } else if (ClassUtils.isShort(parameterType)) {
            int len = buffer.getInt();
            assert len == SHORT_LENGTH;
            return buffer.getShort();
        } else if (ClassUtils.isInt(parameterType)) {
            int len = buffer.getInt();
            assert len == INT_LENGTH;
            return buffer.getInt();
        } else if (ClassUtils.isLong(parameterType)) {
            int len = buffer.getInt();
            assert len == LONG_LENGTH;
            return buffer.getLong();
        } else if (ClassUtils.isChar(parameterType)) {
            int len = buffer.getInt();
            assert len == CHAR_LENGTH;
            return buffer.getChar();
        } else if (ClassUtils.isFloat(parameterType)) {
            int len = buffer.getInt();
            assert len == FLOAT_LENGTH;
            return buffer.getFloat();
        } else if (ClassUtils.isDouble(parameterType)) {
            int len = buffer.getInt();
            assert len == DOUBLE_LENGTH;
            return buffer.getDouble();
        } else {
            throw new RuntimeException("Unsupported parameter type: " + parameterType.getName());
        }
    }
}
