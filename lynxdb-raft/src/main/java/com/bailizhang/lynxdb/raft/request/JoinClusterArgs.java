package com.bailizhang.lynxdb.raft.request;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.ByteBuffer;

public record JoinClusterArgs(
        ServerNode current
) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(true);
        dataBlocks.appendRawStr(current.toString());
        return dataBlocks.toBuffers();
    }
}
