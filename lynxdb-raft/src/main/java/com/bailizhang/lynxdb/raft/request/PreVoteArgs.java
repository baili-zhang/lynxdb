package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

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
}
