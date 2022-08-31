package com.bailizhang.lynxdb.socket.response;

import com.bailizhang.lynxdb.socket.interfaces.Writable;
import com.bailizhang.lynxdb.core.common.BytesConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import static com.bailizhang.lynxdb.core.utils.NumberUtils.INT_LENGTH;

public class WritableSocketResponse extends SocketResponse
        implements Writable, BytesConvertible {

    private final ByteBuffer buffer;

    public WritableSocketResponse(SelectionKey selectionKey, int serial, byte[] data) {
        super(selectionKey);
        this.serial = serial;
        this.data = data;

        buffer = ByteBuffer.wrap(toBytes());
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

    @Override
    public byte[] toBytes() {
        int length = INT_LENGTH * 2 + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);

        return buffer.putInt(data.length)
                .putInt(serial).put(data).array();
    }
}
