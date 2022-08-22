package zbl.moonlight.raft.log;

import java.nio.ByteBuffer;

public record EntryIndex(
        int term,
        int offset,
        int length
) {
    public static final int ENTRY_INDEX_LENGTH = 12;

    public static EntryIndex fromBytes(byte[] bytes) {
        assert bytes.length == ENTRY_INDEX_LENGTH;
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new EntryIndex(buffer.getInt(), buffer.getInt(), buffer.getInt());
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_INDEX_LENGTH);
        return buffer.putInt(term).putInt(offset)
                .putInt(length).array();
    }
}
