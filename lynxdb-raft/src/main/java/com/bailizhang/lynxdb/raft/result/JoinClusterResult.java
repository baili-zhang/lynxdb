package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import static com.bailizhang.lynxdb.ldtp.result.RaftRpcResult.JOIN_CLUSTER_RESULT;

public record JoinClusterResult(
        byte flag
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(JOIN_CLUSTER_RESULT);
        bytesList.appendRawByte(flag);
        return bytesList;
    }
}
