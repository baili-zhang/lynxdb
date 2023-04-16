package com.bailizhang.lynxdb.socket.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.channels.SelectionKey;

public abstract class NioMessage extends NioSelectionKey
        implements BytesListConvertible {

    protected final BytesList bytesList = new BytesList(false);

    public NioMessage(SelectionKey key) {
        super(key);
    }

    @Override
    public BytesList toBytesList() {
        return bytesList;
    }
}
