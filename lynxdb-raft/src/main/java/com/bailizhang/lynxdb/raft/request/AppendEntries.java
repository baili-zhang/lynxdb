package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.raft.log.RaftLogEntry;
import com.bailizhang.lynxdb.raft.state.RaftState;
import com.bailizhang.lynxdb.core.utils.NumberUtils;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class AppendEntries extends RaftRequest {
    private final boolean isClusterMembershipChange;

    public AppendEntries(SelectionKey selectionKey) {
        this(selectionKey, false);
    }

    public AppendEntries(SelectionKey selectionKey, boolean isClusterMembershipChange) {
        super(selectionKey);
        this.isClusterMembershipChange = isClusterMembershipChange;
    }

    public byte[] toBytes() {
        byte[] host = leader().host().getBytes(StandardCharsets.UTF_8);
        int len = NumberUtils.INT_LENGTH * 6 + host.length + 1;
        List<byte[]> entryBytes = new ArrayList<>();

        for(RaftLogEntry raftLogEntry : entries()) {
            byte[] bytes = raftLogEntry.toBytes();
            entryBytes.add(bytes);
            len += NumberUtils.INT_LENGTH + bytes.length;
        }

        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.put(RaftRequest.APPEND_ENTRIES).putInt(host.length)
                .put(host).putInt(leader().port())
                .putInt(term()).putInt(prevLogIndex()).putInt(prevLogTerm())
                .putInt(leaderCommit())
                .put(isClusterMembershipChange ? RaftState.CLUSTER_MEMBERSHIP_CHANGE : RaftState.DATA_CHANGE);

        for(byte[] bytes : entryBytes) {
            buffer.putInt(bytes.length).put(bytes);
        }

        assert buffer.position() == buffer.limit();
        return buffer.array();
    }
}
