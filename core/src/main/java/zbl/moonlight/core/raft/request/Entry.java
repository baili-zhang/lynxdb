package zbl.moonlight.core.raft.request;

import zbl.moonlight.core.utils.NumberUtils;

import java.nio.ByteBuffer;

public record Entry (
        int term,
        int commitIndex,
        byte method,
        byte[] key,
        byte[] value
) {
    public static Entry fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int term = buffer.getInt();
        int commitIndex = buffer.getInt();
        byte method = buffer.get();
        byte[] key = getBytes(buffer);
        byte[] value = getBytes(buffer);
        return new Entry(term, commitIndex,method, key, value);
    }

    public static Entry fromDataBytes(int term, int commitIndex, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte method = buffer.get();
        byte[] key = getBytes(buffer);
        byte[] value = getBytes(buffer);
        return new Entry(term, commitIndex,method, key, value);
    }

    public byte[] toBytes() {
        int len = NumberUtils.INT_LENGTH * 4 + NumberUtils.BYTE_LENGTH + key.length + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putInt(term).putInt(commitIndex).put(method).putInt(key.length)
                .put(key).putInt(value.length).put(value);
        return buffer.array();
    }

    public byte[] getDataBytes() {
        int len = NumberUtils.INT_LENGTH * 2 + NumberUtils.BYTE_LENGTH + key.length + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        return buffer.put(method).putInt(key.length)
                .put(key).putInt(value.length).put(value).array();
    }

    private static byte[] getBytes(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return bytes;
    }
}
