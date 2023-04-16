package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.ByteBuffer;

public record RequestVoteArgs(
        int term,
        ServerNode candidate,
        int lastLogIndex,
        int lastLogTerm
) implements BytesListConvertible {

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawInt(term);
        bytesList.appendVarStr(candidate.toString());

        bytesList.appendRawInt(lastLogIndex);
        bytesList.appendRawInt(lastLogTerm);

        return bytesList;
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
