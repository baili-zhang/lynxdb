package zbl.moonlight.socket.common;

import zbl.moonlight.core.common.BytesConvertible;
import zbl.moonlight.core.common.BytesList;

import java.nio.channels.SelectionKey;

public abstract class NioMessage extends NioSelectionKey implements BytesConvertible {
    protected final BytesList bytesList = new BytesList();

    public NioMessage(SelectionKey key) {
        super(key);
    }

    @Override
    public byte[] toBytes() {
        return bytesList.toBytes();
    }
}
