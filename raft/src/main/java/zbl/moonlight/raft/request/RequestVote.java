package zbl.moonlight.raft.request;

import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.socket.client.ServerNode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

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
        byte[] host = candidate.host().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH * 5 + host.length + 1);
        return buffer.put(RaftRequest.REQUEST_VOTE).putInt(host.length)
                .put(host).putInt(candidate.port()).putInt(term)
                .putInt(lastLogIndex).putInt(lastLogTerm).array();
    }
}
