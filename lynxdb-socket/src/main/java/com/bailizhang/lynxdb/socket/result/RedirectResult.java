package com.bailizhang.lynxdb.socket.result;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.ldtp.result.ResultType.REDIRECT;

public record RedirectResult(ServerNode other) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks();

        dataBlocks.appendRawByte(REDIRECT);
        dataBlocks.appendVarStr(other.toString());

        return dataBlocks.toBuffers();
    }
}
