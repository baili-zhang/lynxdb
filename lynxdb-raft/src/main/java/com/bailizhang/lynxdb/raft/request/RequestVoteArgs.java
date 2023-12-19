package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.ByteBuffer;

public record RequestVoteArgs(
        int term,
        ServerNode candidate,
        int lastLogIndex,
        int lastLogTerm
) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(true);

        dataBlocks.appendRawInt(term);
        dataBlocks.appendVarStr(candidate.toString());

        dataBlocks.appendRawInt(lastLogIndex);
        dataBlocks.appendRawInt(lastLogTerm);

        return dataBlocks.toBuffers();
    }

    public static RequestVoteArgs from(ByteBuffer buffer) {
        int term = buffer.getInt();

        String candidateStr = BufferUtils.getString(buffer);
        ServerNode candidate = ServerNode.from(candidateStr);

        int lastLogIndex = buffer.getInt();
        int lastLogTerm = buffer.getInt();

        return new RequestVoteArgs(term, candidate, lastLogIndex, lastLogTerm);
    }
}
