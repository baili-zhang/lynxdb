package com.bailizhang.lynxdb.socket.request;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public record ByteBufferSocketRequest(
        SelectionKey selectionKey,
        int serial,
        ByteBuffer[] data
) {
    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(true);
        dataBlocks.appendRawInt(serial);
        dataBlocks.appendRawBuffers(data);
        return dataBlocks.toBuffers();
    }
}
