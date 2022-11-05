package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogOptions;

import java.util.concurrent.ConcurrentLinkedDeque;

public class LogRaftState extends BaseRaftState {
    private static final int EXTRA_DATA_LENGTH = 4;

    protected final ConcurrentLinkedDeque<LogEntry> raftLogEntryDeque
            = new ConcurrentLinkedDeque<>();

    protected final LogGroup raftLog = new LogGroup(
            raftConfiguration.logDir(),
            new LogOptions(EXTRA_DATA_LENGTH)
    );

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
