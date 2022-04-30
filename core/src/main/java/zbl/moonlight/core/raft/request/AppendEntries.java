package zbl.moonlight.core.raft.request;

import zbl.moonlight.core.socket.client.ServerNode;

public class AppendEntries extends RaftRequest {
    private final ServerNode leader;
    private final int term;
    private final int prevLogIndex;
    private final int prevLogTerm;
    private final int leaderCommit;
    private final Entry[] entries;

    public AppendEntries(ServerNode leader,
                         int term, int prevLogIndex,
                         int prevLogTerm, int leaderCommit,
                         Entry[] entries) {
        this.leader = leader;
        this.term = term;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }

    @Override
    public byte[] toBytes() {
        return new byte[0];
    }
}
