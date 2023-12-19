package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.REQUEST_VOTE;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.RAFT_RPC;

public record RequestVoteResult(
        int term,
        byte voteGranted
) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);

        dataBlocks.appendRawByte(RAFT_RPC);
        dataBlocks.appendRawByte(REQUEST_VOTE);
        dataBlocks.appendRawInt(term);
        dataBlocks.appendRawByte(voteGranted);

        return dataBlocks.toBuffers();
    }

    public static RequestVoteResult from(ByteBuffer buffer) {
        int term = buffer.getInt();
        byte voteGranted = buffer.get();

        return new RequestVoteResult(term, voteGranted);
    }
}
