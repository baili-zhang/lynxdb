package zbl.moonlight.server.raft.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public record RaftIndexEntry(int term, int commitIndex, int offset, int length) {
    private static final int INDEX_ENTRY_LENGTH = 16;
    private static final int TERM_OFFSET = 0;
    private static final int COMMIT_INDEX_OFFSET = 4;
    private static final int OFFSET_OFFSET = 8;
    private static final int LENGTH_OFFSET = 12;

    public ByteBuffer serialize() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(INDEX_ENTRY_LENGTH);
        byteBuffer.putInt(term);
        byteBuffer.putInt(commitIndex);
        byteBuffer.putInt(offset);
        byteBuffer.putInt(length);

        return byteBuffer;
    }

    private static RaftIndexEntry parse(ByteBuffer byteBuffer) {
        if(byteBuffer.limit() < INDEX_ENTRY_LENGTH) {
            throw new RuntimeException("ByteBuffer limit can not be less than [INDEX_ENTRY_LENGTH].");
        }

        int term = byteBuffer.getInt(TERM_OFFSET);
        int commitIndex = byteBuffer.getInt(COMMIT_INDEX_OFFSET);
        int offset = byteBuffer.getInt(OFFSET_OFFSET);
        int length = byteBuffer.getInt(LENGTH_OFFSET);

        return new RaftIndexEntry(term, commitIndex, offset, length);
    }

    public static RaftIndexEntry read(FileChannel channel, int cursor) throws IOException {
        if(cursor < 0) return null;

        ByteBuffer byteBuffer = ByteBuffer.allocate(INDEX_ENTRY_LENGTH);
        int offset = cursor * INDEX_ENTRY_LENGTH;
        int n = channel.read(byteBuffer, offset);

        if(n == 0 || n == -1) return null;
        if(n < INDEX_ENTRY_LENGTH) throw new RuntimeException("Uncompleted index file.");

        return parse(byteBuffer);
    }

    public static void write(FileChannel channel, ByteBuffer byteBuffer, int cursor) throws IOException {
        channel.write(byteBuffer, ((long) cursor) * ((long) INDEX_ENTRY_LENGTH));
    }
}
