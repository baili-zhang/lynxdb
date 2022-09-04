package com.bailizhang.lynxdb.socket.response;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.socket.common.NioMessage;
import com.bailizhang.lynxdb.socket.interfaces.Writable;
import com.bailizhang.lynxdb.core.common.BytesConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import static com.bailizhang.lynxdb.core.utils.NumberUtils.INT_LENGTH;

public class WritableSocketResponse extends NioMessage implements Writable {
    private final ByteBuffer buffer;

    public WritableSocketResponse(SelectionKey selectionKey, int serial, BytesListConvertible convertible) {
        this(selectionKey, serial, convertible.toBytesList());
    }

    public WritableSocketResponse(SelectionKey selectionKey, int serial, BytesList list) {
        super(selectionKey);
        bytesList.appendRawInt(serial);
        bytesList.append(list);

        buffer = ByteBuffer.wrap(bytesList.toBytes());
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