package com.bailizhang.lynxdb.socket.response;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.common.NioMessage;
import com.bailizhang.lynxdb.socket.interfaces.Writable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WritableSocketResponse extends NioMessage implements Writable {
    private final ByteBuffer[] buffers;
    private final Object extraData;

    public WritableSocketResponse(
            SelectionKey selectionKey,
            int serial,
            ByteBuffer[] data
    ) {
        this(selectionKey, serial, data, null);
    }

    public WritableSocketResponse(
            SelectionKey selectionKey,
            int serial,
            ByteBuffer[] data,
            Object extraData
    ) {
        super(true, selectionKey);
        dataBlocks.appendRawInt(serial);
        dataBlocks.appendRawBuffers(data);

        buffers = dataBlocks.toBuffers();

        this.extraData = extraData;
    }

    public Object extraData() {
        return extraData;
    }

    @Override
    public void write() throws IOException {
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        if(!isWriteCompleted()) {
            channel.write(buffers);
        }
    }

    @Override
    public boolean isWriteCompleted() {
        return BufferUtils.isOver(buffers);
    }
}
