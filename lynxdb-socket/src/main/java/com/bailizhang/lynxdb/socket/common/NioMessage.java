package com.bailizhang.lynxdb.socket.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.channels.SelectionKey;

public abstract class NioMessage extends NioSelectionKey
        implements BytesListConvertible {

    protected final BytesList bytesList;

    public NioMessage(boolean withLength, SelectionKey key) {
        super(key);

        bytesList = new BytesList(withLength);
    }

    public NioMessage(SelectionKey key) {
        this(false, key);
    }

    @Override
    public BytesList toBytesList() {
        return bytesList;
    }
}
