package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import static com.bailizhang.lynxdb.raft.result.RaftResult.LEADER_NOT_EXISTED_RESULT;

public record LeaderNotExistedResult() implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(LEADER_NOT_EXISTED_RESULT);
        return bytesList;
    }
}
