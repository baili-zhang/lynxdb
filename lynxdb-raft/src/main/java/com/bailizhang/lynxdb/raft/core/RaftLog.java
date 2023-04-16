package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.raft.spi.RaftConfiguration;
import com.bailizhang.lynxdb.raft.spi.RaftSpiService;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public class RaftLog {
    private static final RaftLog raftLog = new RaftLog();

    private final LogGroup logGroup;

    private RaftLog() {
        RaftConfiguration raftConfig = RaftSpiService.raftConfig();
        LogGroupOptions options = new LogGroupOptions(INT_LENGTH);
        logGroup = new LogGroup(raftConfig.logsDir(), options);
    }

    public static RaftLog raftLog() {
        return raftLog;
    }

    public int maxIndex() {
        return logGroup.maxGlobalIdx();
    }

    public int maxTerm() {
        byte[] extraData = logGroup.lastExtraData();
        return extraData == null ? 0 : ByteArrayUtils.toInt(extraData);
    }

    public int append(int term, byte[] data) {
        return logGroup.append(BufferUtils.toBytes(term), data);
    }
}
