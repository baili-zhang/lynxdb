package zbl.moonlight.core.raft.response;

import java.nio.ByteBuffer;

public record RaftResponse(byte status, BytesConvertable data) implements BytesConvertable {
    public static final byte REQUEST_VOTE_SUCCESS = (byte) 0x01;
    public static final byte REQUEST_VOTE_FAILURE = (byte) 0x02;
    public static final byte APPEND_ENTRIES_SUCCESS = (byte) 0x03;
    public static final byte APPEND_ENTRIES_FAILURE = (byte) 0x04;

    public static final byte CLIENT_REQUEST_FAILURE = (byte) 0x05;

    @Override
    public byte[] toBytes() {
        byte[] dataBytes = data.toBytes();
        ByteBuffer buffer = ByteBuffer.allocate(1 + dataBytes.length);
        return buffer.put(status).put(dataBytes).array();
    }
}
