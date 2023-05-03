package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import static com.bailizhang.lynxdb.ldtp.result.RaftRpcResult.LEADER_NOT_EXISTED_RESULT;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.LDTP;


public record LeaderNotExistedResult() implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(LDTP);
        bytesList.appendRawByte(LEADER_NOT_EXISTED_RESULT);
        return bytesList;
    }
}
