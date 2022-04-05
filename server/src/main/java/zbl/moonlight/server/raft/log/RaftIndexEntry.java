package zbl.moonlight.server.raft.log;

import java.nio.ByteBuffer;

public record RaftIndexEntry(int term, int commitIndex, int offset, int length) {
    public static final int INDEX_ENTRY_LENGTH = 16;
    public static final int TERM_OFFSET = 0;
    public static final int COMMIT_INDEX_OFFSET = 4;
    public static final int OFFSET_OFFSET = 8;
    public static final int LENGTH_OFFSET = 12;

    public ByteBuffer serialize() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(INDEX_ENTRY_LENGTH);
        byteBuffer.putInt(term);
        byteBuffer.putInt(commitIndex);
        byteBuffer.putInt(offset);
        byteBuffer.putInt(length);

        return byteBuffer;
    }

    public static RaftIndexEntry parse(ByteBuffer byteBuffer) {
        if(byteBuffer.limit() < INDEX_ENTRY_LENGTH) {
            throw new RuntimeException("ByteBuffer limit can not be less than [INDEX_ENTRY_LENGTH].");
        }

        int term = byteBuffer.getInt(TERM_OFFSET);
        int commitIndex = byteBuffer.getInt(COMMIT_INDEX_OFFSET);
        int offset = byteBuffer.getInt(OFFSET_OFFSET);
        int length = byteBuffer.getInt(LENGTH_OFFSET);

        return new RaftIndexEntry(term, commitIndex, offset, length);
    }
}
