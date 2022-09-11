package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.raft.common.RaftLogEntry;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.ByteBuffer;

public record AppendEntriesArgs (
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

        bytesList.appendRawInt(entries.length);
        for(RaftLogEntry entry : entries) {
            bytesList.append(entry);
        }

        bytesList.appendRawInt(leaderCommit);

        return bytesList;
    }

    public static AppendEntriesArgs from(ByteBuffer buffer) {
        int term = buffer.getInt();

        String leaderStr = BufferUtils.getString(buffer);
        ServerNode leader = ServerNode.from(leaderStr);

        int prevLogIndex = buffer.getInt();
        int prevLogTerm = buffer.getInt();

        int entriesSize = buffer.getInt();
        RaftLogEntry[] entries = new RaftLogEntry[entriesSize];
        for(int i = 0; i < entriesSize; i ++) {
            entries[i] = RaftLogEntry.from(buffer);
        }

        int leaderCommit = buffer.getInt();

        return new AppendEntriesArgs(term, leader, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }
}
