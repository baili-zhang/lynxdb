package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.raft.log.LogGroup;
import com.bailizhang.lynxdb.raft.common.RaftLogEntry;

import java.util.concurrent.ConcurrentLinkedDeque;

public class LogRaftState extends BaseRaftState {
    protected final ConcurrentLinkedDeque<RaftLogEntry> raftLogEntryDeque = new ConcurrentLinkedDeque<>();
    protected final LogGroup raftLog = new LogGroup();

    protected LogRaftState() {
    }

    public void appendEntry(RaftLogEntry entry) {
        raftLogEntryDeque.addFirst(entry);
    }

    public int lastLogTerm() {
        return 0;
    }

    public int lastLogIndex() {
        return 0;
    }
}
