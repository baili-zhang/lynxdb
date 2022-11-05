package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public record AppendEntriesArgs (
        int term,
        ServerNode leader,
        int prevLogIndex,
        int prevLogTerm,
        List<LogEntry> entries,
        int leaderCommit
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawInt(term);
        bytesList.appendVarStr(leader.toString());
        bytesList.appendRawInt(prevLogIndex);
        bytesList.appendRawInt(prevLogTerm);

        bytesList.appendRawInt(entries.size());
        for(LogEntry entry : entries) {
            bytesList.appendVarBytes(entry.data());
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
        List<LogEntry> entries = new ArrayList<>();
        for(int i = 0; i < entriesSize; i ++) {
            entries.add(null);
        }

        int leaderCommit = buffer.getInt();

        return new AppendEntriesArgs(term, leader, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }
}
