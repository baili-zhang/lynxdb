package zbl.moonlight.raft.request;

public class ClientRequest extends RaftRequest {
    public final static byte RAFT_CLIENT_REQUEST_GET = (byte) 0x01;
    public final static byte RAFT_CLIENT_REQUEST_SET = (byte) 0x02;

    private final byte[] command;

    public ClientRequest(byte[] command) {
        this.command = command;
    }

    @Override
    public byte[] toBytes() {
        return command;
    }
}
