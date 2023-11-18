package com.bailizhang.lynxdb.core.common;

import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.*;

public class DataBlocks {
    public static final byte RAW = (byte) 0x01;
    public static final byte VAR = (byte) 0x02;

    private final LinkedList<DataBlock<?>> dataBlocks = new LinkedList<>();
    private final boolean withLength;

    /**
     * length 不需要包括自身的 4 bytes，只是记录数据的长度
     */
    private int length;
    private int bufferCount;

    public DataBlocks(boolean withLen) {
        withLength = withLen;
        length = 0;
        bufferCount = withLen ? 1 : 0;
    }

    public void appendRawByte(byte value) {
        append(RAW, value);
    }

    public void appendRawBytes(byte[] value) {
        append(RAW, value);
    }

    public void appendRawBuffers(ByteBuffer[] buffers) {
        for(ByteBuffer buffer : buffers) {
            append(RAW, buffer.array());
        }
    }

    public void appendRawStr(String s) {
        append(RAW, G.I.toBytes(s));
    }

    public void appendRawInt(int value) {
        append(RAW, value);
    }

    public void appendRawLong(long value) {
        append(RAW, value);
    }

    public void appendVarBytes(byte[] value) {
        append(VAR, value);
    }

    public void appendVarStr(String s) {
        append(VAR, G.I.toBytes(s));
    }

    public <V> void append(byte type, V value) {
        dataBlocks.add(new DataBlock<>(type, value));
        length += type == VAR ? INT_LENGTH : 0;

        switch (value) {
            case Integer i -> length += INT_LENGTH;
            case Long l -> length += LONG_LENGTH;
            case Byte b -> length += BYTE_LENGTH;
            case byte[] bytes -> length += bytes.length;
            default -> throw new RuntimeException("Undefined value type");
        }

        bufferCount += type == VAR ? 2 : 1;
    }

    public ByteBuffer[] toBuffers() {
        ByteBuffer[] buffers = new ByteBuffer[bufferCount];

        int idx = 0;
        if(withLength) {
            buffers[0] = BufferUtils.intByteBuffer(length);
            idx = 1;
        }

        for(DataBlock<?> node : dataBlocks) {
            if(node.type == VAR) {
                if(node.value instanceof byte[] bytes) {
                    buffers[idx++] = BufferUtils.intByteBuffer(bytes.length);
                } else {
                    throw new RuntimeException("Undefined value type");
                }
            }

            switch (node.value) {
                case Integer intValue -> buffers[idx++] = BufferUtils.intByteBuffer(intValue);
                case Long longValue -> buffers[idx++] = BufferUtils.longByteBuffer(longValue);
                case Byte byteValue -> buffers[idx++] = BufferUtils.byteByteBuffer(byteValue);
                case byte[] bytes -> buffers[idx++] = ByteBuffer.wrap(bytes);
                default -> throw new RuntimeException("Undefined value type");
            }
        }

        return buffers;
    }

    private static class DataBlock<V> {
        private final byte type;
        private final V value;

        private DataBlock(byte t, V v) {
            type = t;
            value = v;
        }
    }
}
