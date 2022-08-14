package zbl.moonlight.raft.log;

import zbl.moonlight.core.common.BytesConvertible;
import zbl.moonlight.core.utils.NumberUtils;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public record RaftLogEntry (
        SelectionKey selectionKey,
        long serial,
        int term,
        byte type,
        byte[] command
) implements BytesConvertible {
    public static RaftLogEntry fromBytes(SelectionKey selectionKey, byte[] bytes, byte type) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int term = buffer.getInt();
        int len = buffer.limit() - buffer.position();
        byte[] command = new byte[len];
        buffer.get(command);
        return new RaftLogEntry(selectionKey, 0, term, type, command);
    }

    public byte[] toBytes() {
        int len = NumberUtils.INT_LENGTH + command.length;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        return buffer.putInt(term).put(command).array();
    }

    @Override
    public String toString() {
        return String.format("{ term: %d, command: %s }",
                term, new String(command));
    }
}
