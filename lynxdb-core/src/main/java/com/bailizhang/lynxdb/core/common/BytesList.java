package com.bailizhang.lynxdb.core.common;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.BYTE_LENGTH;

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

            if (node.value instanceof Integer) {
                length += PrimitiveTypeUtils.INT_LENGTH;
            } else if (node.value instanceof Long) {
                length += PrimitiveTypeUtils.LONG_LENGTH;
            } else if(node.value instanceof Byte) {
                length += BYTE_LENGTH;
            } else if (node.value instanceof byte[] bytes) {
                length += bytes.length;
            } else {
                throw new RuntimeException("Undefined value type");
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

            if(node.value instanceof Integer i) {
                buffer.putInt(i);
            } else if (node.value instanceof Long l) {
                buffer.putLong(l);
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

    public List<byte[]> toList() {
        LinkedList<byte[]> bytesList = new LinkedList<>();

        for(BytesNode<?> node : bytesNodes) {
            if(node.type == VAR) {
                if(node.value instanceof byte[] bytes) {
                    bytesList.add(BufferUtils.toBytes(bytes.length));
                } else {
                    throw new RuntimeException("Undefined value type");
                }
            }

            if(node.value instanceof Integer i) {
                bytesList.add(BufferUtils.toBytes(i));
            } else if (node.value instanceof Long l) {
                bytesList.add(BufferUtils.toBytes(l));
            } else if(node.value instanceof Byte b) {
                bytesList.add(BufferUtils.toBytes(b));
            } else if (node.value instanceof byte[] bytes) {
                bytesList.add(bytes);
            } else {
                throw new RuntimeException("Undefined value type");
            }
        }

        if(withLength) {
            int length = bytesList.stream().mapToInt(bytes -> bytes.length).sum();
            bytesList.addFirst(BufferUtils.toBytes(length));
        }

        return bytesList;
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
