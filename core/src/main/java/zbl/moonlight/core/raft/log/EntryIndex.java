package zbl.moonlight.core.raft.log;

import zbl.moonlight.core.raft.request.Entry;

import java.nio.ByteBuffer;

public record EntryIndex(
        int term,
        int commitIndex,
        int offset,
        int length
) {
    public static final int ENTRY_INDEX_LENGTH = 16;

    public static Entry fromBytes(byte[] bytes) {
        return null;
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_INDEX_LENGTH);
        return buffer.putInt(term).putInt(commitIndex).putInt(offset)
                .putInt(length).array();
    }
}
