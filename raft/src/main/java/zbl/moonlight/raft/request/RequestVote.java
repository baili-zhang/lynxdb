package zbl.moonlight.raft.request;

import zbl.moonlight.core.utils.NumberUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RequestVote extends RaftRequest {
    public RequestVote() {
    }

    @Override
    public byte[] toBytes() {
        byte[] host = candidate().host().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH * 5 + host.length + 1);
        return buffer.put(RaftRequest.REQUEST_VOTE).putInt(host.length)
                .put(host).putInt(candidate().port()).putInt(term())
                .putInt(lastLogIndex()).putInt(lastLogTerm()).array();
    }
}
