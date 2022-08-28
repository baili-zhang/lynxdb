package zbl.moonlight.socket.common;

import zbl.moonlight.core.common.BytesConvertible;
import zbl.moonlight.core.common.BytesList;
import zbl.moonlight.core.common.BytesListConvertible;

import java.nio.channels.SelectionKey;

public abstract class NioMessage extends NioSelectionKey
        implements BytesListConvertible {

    protected final BytesList bytesList = new BytesList();

    public NioMessage(SelectionKey key) {
        super(key);
    }

    @Override
    public BytesList toBytesList() {
        return bytesList;
    }
}
