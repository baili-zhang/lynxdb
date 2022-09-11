package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
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
) implements BytesListConvertible {
    public static final byte NOT_DONE = (byte) 0x01;
    public static final byte IS_DONE = (byte) 0x02;

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawInt(term);
        bytesList.appendVarStr(leader.toString());
        bytesList.appendRawInt(lastIncludedIndex);
        bytesList.appendRawInt(lastIncludedTerm);
        bytesList.appendRawInt(offset);
        bytesList.appendVarBytes(data);
        bytesList.appendRawByte(done);

        return bytesList;
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
