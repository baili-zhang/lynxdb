package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.ByteBuffer;

public record PreVoteArgs(
        int term,
        int lastLogIndex,
        int lastLogTerm
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawInt(term);
        bytesList.appendRawInt(lastLogIndex);
        bytesList.appendRawInt(lastLogTerm);

        return bytesList;
    }

    public static PreVoteArgs from(ByteBuffer buffer) {
        int term = buffer.getInt();
        int lastLogIndex = buffer.getInt();
        int lastLogTerm = buffer.getInt();

        return new PreVoteArgs(term, lastLogIndex, lastLogTerm);
    }
}
