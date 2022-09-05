package com.bailizhang.lynxdb.core.common;

import com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils;

import java.nio.ByteBuffer;
import java.util.LinkedList;

public class BytesList implements BytesConvertible{
    public static final byte RAW = (byte) 0x01;
    public static final byte VAR = (byte) 0x02;

    private final LinkedList<BytesNode<?>> bytesNodes = new LinkedList<>();

    public BytesList() {

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
        int length = PrimitiveTypeUtils.INT_LENGTH;
        for(BytesNode<?> node : bytesNodes) {
            if(node.type == VAR) {
                length += PrimitiveTypeUtils.INT_LENGTH;
            }

            if(node.value instanceof Integer) {
                length += PrimitiveTypeUtils.INT_LENGTH;
            } else if(node.value instanceof Byte) {
                length += PrimitiveTypeUtils.BYTE_LENGTH;
            } else if (node.value instanceof byte[] bytes) {
                length += bytes.length;
            } else {
                throw new RuntimeException("Undefined value type");
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(length);
        buffer.putInt(length - PrimitiveTypeUtils.INT_LENGTH);
        for(BytesNode<?> node : bytesNodes) {
            if(node.type == VAR) {
                if(node.value instanceof byte[] bytes) {
                    buffer.putInt(bytes.length);
                } else {
                    throw new RuntimeException("Undefined value type");
                }
            }

            if(node.value instanceof Integer i) {
                buffer.putInt(i);
            } else if(node.value instanceof Byte b) {
                buffer.put(b);
            } else if (node.value instanceof byte[] bytes) {
                buffer.put(bytes);
            } else {
                throw new RuntimeException("Undefined value type");
            }
        }

        return buffer.array();
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
