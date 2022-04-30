package zbl.moonlight.core.raft.request;

import zbl.moonlight.core.raft.response.BytesConvertable;
import zbl.moonlight.core.utils.NumberUtils;

import java.nio.ByteBuffer;

public record Entry (
        int term,
        byte[] command
) implements BytesConvertable {
    public static Entry fromBytes(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int term = buffer.getInt();
        int len = buffer.limit() - buffer.position();
        byte[] command = new byte[len];
        buffer.get(command);
        return new Entry(term, command);
    }

    public byte[] toBytes() {
        int len = NumberUtils.INT_LENGTH + command.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        return buffer.putInt(term).put(command).array();
    }
}
