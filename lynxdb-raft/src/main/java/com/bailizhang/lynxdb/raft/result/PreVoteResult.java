package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.ldtp.result.RaftRpcResult.PRE_VOTE_RESULT;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.RAFT_RPC;

public record PreVoteResult(
        int term,
        byte voteGranted
) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);

        dataBlocks.appendRawByte(RAFT_RPC);
        dataBlocks.appendRawByte(PRE_VOTE_RESULT);
        dataBlocks.appendRawInt(term);
        dataBlocks.appendRawByte(voteGranted);

        return dataBlocks.toBuffers();
    }

    public static PreVoteResult from(ByteBuffer buffer) {
        int term = buffer.getInt();
        byte voteGranted = buffer.get();

        return new PreVoteResult(term, voteGranted);
    }
}
