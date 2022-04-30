package zbl.moonlight.core.raft.request;

import zbl.moonlight.core.socket.client.ServerNode;

public class RequestVote extends RaftRequest {
    private final ServerNode candidate;
    private final int term;
    private final int lastLogIndex;
    private final int lastLogTerm;

    public RequestVote(ServerNode candidate,
                       int term, int lastLogIndex,
                       int lastLogTerm) {
        this.candidate = candidate;
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public byte[] toBytes() {

        return new byte[0];
    }
}
