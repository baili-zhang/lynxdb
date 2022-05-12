package zbl.moonlight.core.raft.request;

import lombok.ToString;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.utils.NumberUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@ToString
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
        byte[] host = leader.host().getBytes(StandardCharsets.UTF_8);
        int len = NumberUtils.INT_LENGTH * 6 + host.length + 1;
        List<byte[]> entryBytes = new ArrayList<>();

        for(Entry entry : entries) {
            byte[] bytes = entry.toBytes();
            entryBytes.add(bytes);
            len += NumberUtils.INT_LENGTH + bytes.length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.put(RaftRequest.APPEND_ENTRIES).putInt(host.length)
                .put(host).putInt(leader.port())
                .putInt(term).putInt(prevLogIndex).putInt(prevLogTerm)
                .putInt(leaderCommit);

        for(byte[] bytes : entryBytes) {
            buffer.putInt(bytes.length).put(bytes);
        }

        assert buffer.position() == buffer.limit();
        return buffer.array();
    }
}
