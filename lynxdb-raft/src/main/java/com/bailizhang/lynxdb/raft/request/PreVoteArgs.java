package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;

public record PreVoteArgs(
        int term,
        int lastLogIndex,
        int lastLogTerm
) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks();

        dataBlocks.appendRawInt(term);
        dataBlocks.appendRawInt(lastLogIndex);
        dataBlocks.appendRawInt(lastLogTerm);

        return dataBlocks.toBuffers();
    }

    public static PreVoteArgs from(ByteBuffer buffer) {
        int term = buffer.getInt();
        int lastLogIndex = buffer.getInt();
        int lastLogTerm = buffer.getInt();

        return new PreVoteArgs(term, lastLogIndex, lastLogTerm);
    }
}
