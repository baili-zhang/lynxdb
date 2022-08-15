package zbl.moonlight.raft.request;

import java.nio.channels.SelectionKey;

public class ClientRequest extends RaftRequest {
    public final static byte RAFT_CLIENT_REQUEST_GET = (byte) 0x01;
    public final static byte RAFT_CLIENT_REQUEST_SET = (byte) 0x02;

    public ClientRequest(SelectionKey selectionKey) {
        super(selectionKey);
    }

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }
}
