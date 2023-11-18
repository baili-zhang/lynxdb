package com.bailizhang.lynxdb.socket.request;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.common.NioMessage;
import com.bailizhang.lynxdb.socket.common.NioSelectionKey;
import com.bailizhang.lynxdb.socket.interfaces.Writable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WritableSocketRequest extends NioSelectionKey implements Writable {
    private final ByteBuffer[] buffers;

    public WritableSocketRequest(
            SelectionKey key,
            byte status,
            int serial,
            ByteBuffer[] data
    ) {
        super(key);

        DataBlocks dataBlocks = new DataBlocks();
        dataBlocks.appendRawByte(status);
        dataBlocks.appendRawInt(serial);
        dataBlocks.appendRawBuffers(data);

        buffers = dataBlocks.toBuffers();
    }

    public WritableSocketRequest(
            byte status,
            int serial,
            NioMessage message
    ) {
        super(message.selectionKey());

        DataBlocks dataBlocks = new DataBlocks();
        dataBlocks.appendRawByte(status);
        dataBlocks.appendRawInt(serial);
        dataBlocks.appendRawBuffers(message.toBuffers());

        buffers = dataBlocks.toBuffers();
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
