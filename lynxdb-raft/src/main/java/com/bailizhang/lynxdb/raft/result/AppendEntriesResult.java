package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.APPEND_ENTRIES;

public record AppendEntriesResult(
        int term,
        byte success
) implements BytesListConvertible {
    public static final byte IS_SUCCESS = (byte) 0x01;
    public static final byte IS_FAILED = (byte) 0x02;

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(APPEND_ENTRIES);
        bytesList.appendRawInt(term);
        bytesList.appendRawByte(success);

        return bytesList;
    }

    public static AppendEntriesResult from(ByteBuffer buffer) {
        int term = buffer.getInt();
        byte success = buffer.get();

        return new AppendEntriesResult(term, success);
    }
}
