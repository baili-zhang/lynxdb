package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.DataBlocks;
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
) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(true);

        dataBlocks.appendRawInt(term);
        dataBlocks.appendVarStr(leader.toString());
        dataBlocks.appendRawInt(prevLogIndex);
        dataBlocks.appendRawInt(prevLogTerm);

        dataBlocks.appendRawInt(entries.size());
        for(LogEntry entry : entries) {
            dataBlocks.appendVarBytes(entry.data());
        }

        dataBlocks.appendRawInt(leaderCommit);

        return dataBlocks.toBuffers();
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
