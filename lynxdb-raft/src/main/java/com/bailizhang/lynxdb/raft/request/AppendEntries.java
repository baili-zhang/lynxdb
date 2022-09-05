package com.bailizhang.lynxdb.raft.request;

import java.nio.channels.SelectionKey;

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
