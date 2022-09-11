package com.bailizhang.lynxdb.raft.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;


public record RaftLogEntry (
        SelectionKey selectionKey,
        Integer serial,
        int term,
        byte type,
        byte[] command
) implements BytesListConvertible {

    public final static byte DATA_CHANGE = (byte) 0x01;
    public final static byte CLUSTER_MEMBERSHIP_CHANGE = (byte) 0x02;

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawInt(term);
        bytesList.appendVarBytes(command);

        return bytesList;
    }

    public static RaftLogEntry from(ByteBuffer buffer) {
        int term = buffer.getInt();
        byte[] command = BufferUtils.getBytes(buffer);

        return new RaftLogEntry(null, null, term, DATA_CHANGE, command);
    }
}
