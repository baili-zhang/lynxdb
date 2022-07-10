package zbl.moonlight.raft.request;

import zbl.moonlight.core.common.BytesConvertible;

public abstract class RaftRequest implements BytesConvertible {
    public final static byte REQUEST_VOTE = (byte) 0x01;
    public final static byte APPEND_ENTRIES = (byte) 0x02;
    public final static byte CLIENT_REQUEST = (byte) 0x03;

    abstract public byte[] toBytes();
}
