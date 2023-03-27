package com.bailizhang.lynxdb.raft.core;

import java.nio.channels.SelectionKey;

public class RaftRpcResultHandler {
    public void handleRequestVoteResult(
            SelectionKey selectionKey,
            int term,
            byte voteGranted
    ) {

    }

    public void handleAppendEntriesResult(
            SelectionKey selectionKey,
            int term,
            byte voteGranted
    ) {

    }

    public void handleInstallSnapshotResult(SelectionKey selectionKey, int term) {
    }
}
