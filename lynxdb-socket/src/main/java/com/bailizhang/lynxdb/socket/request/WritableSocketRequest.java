package com.bailizhang.lynxdb.socket.request;

import com.bailizhang.lynxdb.socket.interfaces.Writable;
import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.common.NioMessage;
import com.bailizhang.lynxdb.socket.common.NioSelectionKey;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WritableSocketRequest extends NioSelectionKey implements Writable {
    private final ByteBuffer buffer;

    public WritableSocketRequest(SelectionKey key, byte status,
                                 int serial, byte[] data) {
        super(key);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(status);
        bytesList.appendRawInt(serial);
        bytesList.appendRawBytes(data);

        buffer = ByteBuffer.wrap(bytesList.toBytes());
    }

    public WritableSocketRequest(byte status, int serial,
                                 NioMessage message) {
        super(message.selectionKey());

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(status);
        bytesList.appendRawInt(serial);
        bytesList.append(message);

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
