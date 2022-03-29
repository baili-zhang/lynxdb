package zbl.moonlight.core.protocol.nio;

import lombok.Getter;
import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Serializer;
import zbl.moonlight.core.protocol.Writable;
import zbl.moonlight.core.utils.ByteBufferUtils;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class NioWriter extends Serializer implements Writable {
    private boolean wrapped = false;

    @Getter
    private final SelectionKey selectionKey;

    public NioWriter(Class<? extends MSerializable> schemaClass, SelectionKey selectionKey) {
        super(schemaClass);
        this.selectionKey = selectionKey;
    }

    @Override
    public void write() throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        if(!wrapped) {
            wrap();
        }

        if(!ByteBufferUtils.isOver(byteBuffer)) {
            socketChannel.write(byteBuffer);
        }
    }

    @Override
    public boolean isWriteCompleted() {
        return ByteBufferUtils.isOver(byteBuffer);
    }

    public void wrap() {
        serialize();
        /* 设置标志 */
        wrapped = true;
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
