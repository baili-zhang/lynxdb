package com.bailizhang.lynxdb.socket.common;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public abstract class NioMessage extends NioSelectionKey {

    protected final DataBlocks dataBlocks;

    public NioMessage(boolean withLength, SelectionKey key) {
        super(key);

        dataBlocks = new DataBlocks(withLength);
    }

    public NioMessage(SelectionKey key) {
        this(false, key);
    }

    public ByteBuffer[] toBuffers() {
        return dataBlocks.toBuffers();
    }
}
