package zbl.moonlight.core.raft.request;

public class ClientRequest extends RaftRequest {
    @Override
    public byte[] toBytes() {
        return new byte[0];
    }
}
