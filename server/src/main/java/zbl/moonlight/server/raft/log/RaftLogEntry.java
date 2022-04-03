package zbl.moonlight.server.raft.log;

import zbl.moonlight.core.protocol.Serializer;

import java.nio.ByteBuffer;

public record RaftLogEntry(byte method, byte[] key, byte[] value) {
    public ByteBuffer serialize() {
        Serializer serializer = new Serializer(RaftDataLogSchema.class, false);
        serializer.mapPut(RaftDataLogSchema.METHOD, new byte[]{method});
        serializer.mapPut(RaftDataLogSchema.KEY, key);
        serializer.mapPut(RaftDataLogSchema.VALUE, value);
        return serializer.getByteBuffer();
    }
}
