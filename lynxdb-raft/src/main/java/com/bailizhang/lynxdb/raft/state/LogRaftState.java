package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.raft.log.LogEntry;
import com.bailizhang.lynxdb.raft.log.LogGroup;

import java.util.concurrent.ConcurrentLinkedDeque;

public class LogRaftState extends BaseRaftState {
    protected final ConcurrentLinkedDeque<LogEntry> raftLogEntryDeque = new ConcurrentLinkedDeque<>();
    protected final LogGroup raftLog = new LogGroup(raftConfiguration.logDir());

    protected LogRaftState() {
    }

    public void appendEntry(LogEntry entry) {
        raftLogEntryDeque.addFirst(entry);
    }

    public int lastLogTerm() {
        return 0;
    }

    public int lastLogIndex() {
        return 0;
    }
}
