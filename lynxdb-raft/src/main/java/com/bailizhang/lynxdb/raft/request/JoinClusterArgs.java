package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.socket.client.ServerNode;

public record JoinClusterArgs(
        ServerNode current
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();
        bytesList.appendRawStr(current.toString());
        return bytesList;
    }
}
