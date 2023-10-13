package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.raft.spi.RaftConfiguration;
import com.bailizhang.lynxdb.raft.spi.RaftSpiService;

public class RaftLog {
    private static final RaftLog raftLog = new RaftLog();

    private final LogGroup logGroup;

    private RaftLog() {
        RaftConfiguration raftConfig = RaftSpiService.raftConfig();
        LogGroupOptions options = new LogGroupOptions();
        logGroup = new LogGroup(raftConfig.logsDir(), options);
    }

    public static RaftLog raftLog() {
        return raftLog;
    }

    public int maxIndex() {
        return logGroup.maxGlobalIdx();
    }

    public int maxTerm() {
        return 0;
    }

    public int append(int term, byte[] data) {
        return logGroup.appendEntry(data);
    }
}
