package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.socket.client.ServerNode;

public record RequestVoteArgs(
        int term,
        ServerNode candidate,
        int lastLogIndex,
        int lastLogTerm
) implements BytesListConvertible {

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendVarStr(candidate.toString());

        bytesList.appendRawInt(lastLogIndex);
        bytesList.appendRawInt(lastLogTerm);

        return bytesList;
    }
}
