package zbl.moonlight.core.raft.request;

public interface RaftRequest {
    byte REQUEST_VOTE = (byte) 0x01;
    byte APPEND_ENTRIES = (byte) 0x02;
    byte CLIENT_REQUEST = (byte) 0x03;
}
