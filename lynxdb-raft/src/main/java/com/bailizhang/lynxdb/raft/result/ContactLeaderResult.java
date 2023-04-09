package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import static com.bailizhang.lynxdb.raft.result.RaftResult.CONTACT_LEADER_RESULT;

public record ContactLeaderResult(
        byte flag
) implements BytesListConvertible {
    public static final byte IS_SUCCESS = (byte) 0x01;
    public static final byte IS_FAILED = (byte) 0x02;

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(CONTACT_LEADER_RESULT);
        bytesList.appendRawByte(flag);
        return bytesList;
    }
}
