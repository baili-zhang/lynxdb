package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.ldtp.result.RaftRpcResult.INSTALL_SNAPSHOT_RESULT;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.RAFT_RPC;


public record InstallSnapshotResult(
        int term
) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);

        dataBlocks.appendRawByte(RAFT_RPC);
        dataBlocks.appendRawByte(INSTALL_SNAPSHOT_RESULT);
        dataBlocks.appendRawInt(term);

        return dataBlocks.toBuffers();
    }

    public static InstallSnapshotResult from(ByteBuffer buffer) {
        int term = buffer.get();

        return new InstallSnapshotResult(term);
    }
}
