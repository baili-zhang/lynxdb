package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.core.file.LogFileGroup;
import com.bailizhang.lynxdb.raft.common.RaftLogEntry;

import java.util.concurrent.ConcurrentLinkedDeque;

public class LogRaftState extends BaseRaftState {
    protected final ConcurrentLinkedDeque<RaftLogEntry> raftLogEntryDeque = new ConcurrentLinkedDeque<>();
    protected final LogFileGroup raftLog = new LogFileGroup(raftConfiguration.logDir());

    protected LogRaftState() {
    }

    public void appendEntry(RaftLogEntry entry) {
        raftLog.append(entry);
        raftLogEntryDeque.addFirst(entry);
    }
}
