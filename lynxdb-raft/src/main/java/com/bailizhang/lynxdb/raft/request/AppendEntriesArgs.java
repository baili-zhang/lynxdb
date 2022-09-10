package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.raft.state.RaftLogEntry;
import com.bailizhang.lynxdb.socket.client.ServerNode;

public record AppendEntriesArgs(
        int term,
        ServerNode leader,
        int prevLogIndex,
        int prevLogTerm,
        RaftLogEntry[] entries,
        int leaderCommit
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawInt(term);
        bytesList.appendVarStr(leader.toString());
        bytesList.appendRawInt(prevLogIndex);
        bytesList.appendRawInt(prevLogTerm);

        for(RaftLogEntry entry : entries) {
            bytesList.append(entry);
        }

        bytesList.appendRawInt(leaderCommit);

        return bytesList;
    }
}
