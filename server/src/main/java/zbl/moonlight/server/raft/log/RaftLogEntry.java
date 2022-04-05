package zbl.moonlight.server.raft.log;

import zbl.moonlight.core.protocol.Serializer;

import java.nio.ByteBuffer;

public record RaftLogEntry(int term, int commitIndex, byte method, byte[] key, byte[] value) {
    public ByteBuffer serializeData() {
        Serializer serializer = new Serializer(RaftDataLogSchema.class, false);
        serializer.mapPut(RaftDataLogSchema.METHOD, new byte[]{method});
        serializer.mapPut(RaftDataLogSchema.KEY, key);
        serializer.mapPut(RaftDataLogSchema.VALUE, value);
        return serializer.getByteBuffer();
    }

    public ByteBuffer serializeIndex(int offset, int length) {
        ByteBuffer byteBuffer = new RaftIndexEntry(term, commitIndex, offset, length).serialize();
        byteBuffer.rewind();
        return byteBuffer;
    }
}
