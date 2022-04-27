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
        return null;
    }

    public byte[] toBytes() {
        int len = NumberUtils.INT_LENGTH * 4 + NumberUtils.BYTE_LENGTH + key.length + value.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.putInt(term).putInt(commitIndex).put(method).putInt(key.length)
                .put(key).putInt(value.length).put(value);
        return buffer.array();
    }
}
