package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.APPEND_ENTRIES;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.RAFT_RPC;

public record AppendEntriesResult(
        int term,
        byte success
) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);

        dataBlocks.appendRawByte(RAFT_RPC);
        dataBlocks.appendRawByte(APPEND_ENTRIES);
        dataBlocks.appendRawInt(term);
        dataBlocks.appendRawByte(success);

        return dataBlocks.toBuffers();
    }

    public static AppendEntriesResult from(ByteBuffer buffer) {
        int term = buffer.getInt();
        byte success = buffer.get();

        return new AppendEntriesResult(term, success);
    }
}
