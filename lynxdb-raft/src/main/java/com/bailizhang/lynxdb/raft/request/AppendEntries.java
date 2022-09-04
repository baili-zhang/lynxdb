package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.raft.state.RaftLogEntry;
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
}
