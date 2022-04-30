package zbl.moonlight.core.raft.request;

public class ClientRequest extends RaftRequest {
    private final byte[] command;

    public ClientRequest(byte[] command) {
        this.command = command;
    }

    @Override
    public byte[] toBytes() {
        return command;
    }
}
