package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.ldtp.result.RaftRpcResult.LEADER_NOT_EXISTED_RESULT;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.LDTP;


public record LeaderNotExistedResult() {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte(LDTP);
        dataBlocks.appendRawByte(LEADER_NOT_EXISTED_RESULT);
        return dataBlocks.toBuffers();
    }
}
