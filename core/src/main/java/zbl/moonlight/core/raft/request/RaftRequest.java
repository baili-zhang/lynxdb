package zbl.moonlight.core.raft.request;

import zbl.moonlight.core.raft.response.BytesConvertable;

public abstract class RaftRequest implements BytesConvertable {
    public final static byte REQUEST_VOTE = (byte) 0x01;
    public final static byte APPEND_ENTRIES = (byte) 0x02;
    public final static byte CLIENT_REQUEST = (byte) 0x03;

    abstract public byte[] toBytes();
}
