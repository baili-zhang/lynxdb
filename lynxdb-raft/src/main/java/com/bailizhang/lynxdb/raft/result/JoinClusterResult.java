package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.ldtp.result.RaftRpcResult.JOIN_CLUSTER_RESULT;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.RAFT_RPC;

public record JoinClusterResult(
        byte flag
) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte(RAFT_RPC);
        dataBlocks.appendRawByte(JOIN_CLUSTER_RESULT);
        dataBlocks.appendRawByte(flag);
        return dataBlocks.toBuffers();
    }
}
