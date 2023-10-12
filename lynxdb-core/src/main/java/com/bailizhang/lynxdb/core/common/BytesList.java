package com.bailizhang.lynxdb.core.common;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class BytesList implements BytesConvertible {
    public static final byte RAW = (byte) 0x01;
    public static final byte VAR = (byte) 0x02;

    private final LinkedList<BytesNode<?>> bytesNodes = new LinkedList<>();

    private final boolean withLength;

    public BytesList() {
        withLength = true;
    }

    public BytesList(boolean withLen) {
        withLength = withLen;
    }

    public void appendRawByte(byte value) {
        append(RAW, value);
    }

    public void appendRawBytes(byte[] value) {
        append(RAW, value);
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
        bytesNodes.add(new BytesNode<>(type, value));
    }

    public void append(BytesList list) {
        bytesNodes.addAll(list.bytesNodes);
    }

    public void append(BytesListConvertible convertible) {
        BytesList list = convertible.toBytesList();
        append(list);
    }

    @Override
    public byte[] toBytes() {
        int length = withLength ? PrimitiveTypeUtils.INT_LENGTH : 0;
        for(BytesNode<?> node : bytesNodes) {
            if(node.type == VAR) {
                length += PrimitiveTypeUtils.INT_LENGTH;
            }

            switch (node.value) {
                case Integer i -> length += PrimitiveTypeUtils.INT_LENGTH;
                case Long l -> length += PrimitiveTypeUtils.LONG_LENGTH;
                case Byte b -> length += PrimitiveTypeUtils.BYTE_LENGTH;
                case byte[] bytes -> length += bytes.length;
                default -> throw new RuntimeException("Undefined value type");
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(length);

        if(withLength) {
            buffer.putInt(length - PrimitiveTypeUtils.INT_LENGTH);
        }

        for(BytesNode<?> node : bytesNodes) {
            if(node.type == VAR) {
                if(node.value instanceof byte[] bytes) {
                    buffer.putInt(bytes.length);
                } else {
                    throw new RuntimeException("Undefined value type");
                }
            }

            switch (node.value) {
                case Integer i -> buffer.putInt(i);
                case Long l -> buffer.putLong(l);
                case Byte b -> buffer.put(b);
                case byte[] bytes -> buffer.put(bytes);
                default -> throw new RuntimeException("Undefined value type");
            }
        }

        return buffer.array();
    }

    /**
     * 返回 byte[] list
     * 主要是为了减少内存的拷贝
     *
     * @return bytes list
     */
    public List<byte[]> toList() {
        LinkedList<byte[]> list = new LinkedList<>();

        int length = withLength ? PrimitiveTypeUtils.INT_LENGTH : 0;

        for(BytesNode<?> node : bytesNodes) {
            if(node.type == VAR) {
                length += PrimitiveTypeUtils.INT_LENGTH;
            }

            switch (node.value) {
                case Integer i -> {
                    length += PrimitiveTypeUtils.INT_LENGTH;
                    list.add(BufferUtils.toBytes(i));
                }
                case Long l -> {
                    length += PrimitiveTypeUtils.LONG_LENGTH;
                    list.add(BufferUtils.toBytes(l));
                }
                case Byte b -> {
                    length += PrimitiveTypeUtils.BYTE_LENGTH;
                    list.add(new byte[]{b});
                }
                case byte[] bytes -> {
                    length += bytes.length;
                    if(node.type == VAR) {
                        list.add(BufferUtils.toBytes(bytes.length));
                    }
                    list.add(bytes);
                }
                default -> throw new RuntimeException("Undefined value type");
            }
        }

        if(withLength) {
            list.addFirst(BufferUtils.toBytes(length));
        }

        return list;
    }

    private static class BytesNode<V> {
        private final byte type;
        private final V value;

        private BytesNode(byte t, V v) {
            type = t;
            value = v;
        }
    }
}
