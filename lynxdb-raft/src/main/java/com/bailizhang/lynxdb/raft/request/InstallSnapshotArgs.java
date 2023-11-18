package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.ByteBuffer;

public record InstallSnapshotArgs (
        int term,
        ServerNode leader,
        int lastIncludedIndex,
        int lastIncludedTerm,
        int offset,
        byte[] data,
        byte done
) {
    public static final byte NOT_DONE = (byte) 0x01;
    public static final byte IS_DONE = (byte) 0x02;

    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(true);

        dataBlocks.appendRawInt(term);
        dataBlocks.appendVarStr(leader.toString());
        dataBlocks.appendRawInt(lastIncludedIndex);
        dataBlocks.appendRawInt(lastIncludedTerm);
        dataBlocks.appendRawInt(offset);
        dataBlocks.appendVarBytes(data);
        dataBlocks.appendRawByte(done);

        return dataBlocks.toBuffers();
    }

    public static InstallSnapshotArgs from(ByteBuffer buffer) {
        int term = buffer.getInt();

        String leaderStr = BufferUtils.getString(buffer);
        ServerNode leader = ServerNode.from(leaderStr);

        int lastIncludedIndex = buffer.getInt();
        int lastIncludedTerm = buffer.getInt();
        int offset = buffer.getInt();
        byte[] data = BufferUtils.getBytes(buffer);
        byte done = buffer.get();

        return new InstallSnapshotArgs(term, leader, lastIncludedIndex,
                lastIncludedTerm, offset, data, done);
    }
}
