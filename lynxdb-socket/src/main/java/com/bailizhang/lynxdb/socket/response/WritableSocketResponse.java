package com.bailizhang.lynxdb.socket.response;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.common.NioMessage;
import com.bailizhang.lynxdb.socket.interfaces.Writable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WritableSocketResponse extends NioMessage implements Writable {
    private final ByteBuffer buffer;
    private final Object extraData;

    public WritableSocketResponse(
            SelectionKey selectionKey,
            int serial,
            BytesList list
    ) {
        this(selectionKey, serial, list, null);
    }

    public WritableSocketResponse(
            SelectionKey selectionKey,
            int serial,
            BytesListConvertible convertible
    ) {
        this(selectionKey, serial, convertible.toBytesList(), null);
    }

    public WritableSocketResponse(
            SelectionKey selectionKey,
            int serial,
            BytesListConvertible convertible,
            Object extraData
    ) {
        this(selectionKey, serial, convertible.toBytesList(), extraData);
    }

    public WritableSocketResponse(
            SelectionKey selectionKey,
            int serial,
            BytesList list,
            Object extraData
    ) {
        super(selectionKey);
        bytesList.appendRawInt(serial);
        bytesList.append(list);

        buffer = ByteBuffer.wrap(bytesList.toBytes());

        this.extraData = extraData;
    }

    public Object extraData() {
        return extraData;
    }

    @Override
    public void write() throws IOException {
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        if(!isWriteCompleted()) {
            channel.write(buffer);
        }
    }

    @Override
    public boolean isWriteCompleted() {
        return BufferUtils.isOver(buffer);
    }
}
