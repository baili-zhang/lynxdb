package zbl.moonlight.raft.request;

import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.raft.log.Entry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class AppendEntries extends RaftRequest {
    private final static byte DATA_CHANGE = (byte) 0x01;
    private final static byte CLUSTER_MEMBERSHIP_CHANGE = (byte) 0x02;

    private final boolean isClusterMembershipChange;

    public AppendEntries() {
        this(false);
    }

    public AppendEntries(boolean isClusterMembershipChange) {
        this.isClusterMembershipChange = isClusterMembershipChange;
    }

    @Override
    public byte[] toBytes() {
        byte[] host = leader().host().getBytes(StandardCharsets.UTF_8);
        int len = NumberUtils.INT_LENGTH * 6 + host.length + 1;
        List<byte[]> entryBytes = new ArrayList<>();

        for(Entry entry : entries()) {
            byte[] bytes = entry.toBytes();
            entryBytes.add(bytes);
            len += NumberUtils.INT_LENGTH + bytes.length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.put(RaftRequest.APPEND_ENTRIES).putInt(host.length)
                .put(host).putInt(leader().port())
                .putInt(term()).putInt(prevLogIndex()).putInt(prevLogTerm())
                .putInt(leaderCommit())
                .put(isClusterMembershipChange ? CLUSTER_MEMBERSHIP_CHANGE : DATA_CHANGE);

        for(byte[] bytes : entryBytes) {
            buffer.putInt(bytes.length).put(bytes);
        }

        assert buffer.position() == buffer.limit();
        return buffer.array();
    }
}
