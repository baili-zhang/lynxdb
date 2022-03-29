package zbl.moonlight.core.protocol.nio;

import lombok.Getter;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.Parser;
import zbl.moonlight.core.protocol.Readable;
import zbl.moonlight.core.utils.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

/** 协议的读策略，需要提供继承Parsable的接口 */
public class NioReader extends Parser implements Readable {
    @Getter
    private final SelectionKey selectionKey;
    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);

    /** 给读数据用的构造函数 */
    public NioReader(Class<? extends Parsable> schemaClass, SelectionKey selectionKey) {
        super(schemaClass);
        this.selectionKey = selectionKey;
    }

    @Override
    public void read() throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        if (!ByteBufferUtils.isOver(lengthBuffer)) {
            socketChannel.read(lengthBuffer);
            if (!ByteBufferUtils.isOver(lengthBuffer)) {
                return;
            }
            int length = lengthBuffer.rewind().getInt();
            byteBuffer = ByteBuffer.allocate(length);
        }

        if(!isReadCompleted()) {
            socketChannel.read(byteBuffer);
            /* 如果读取完成，则解析读取的数据 */
            if(isReadCompleted()) {
                parse();
            }
        }
    }

    @Override
    public boolean isReadCompleted() {
        return ByteBufferUtils.isOver(byteBuffer);
    }

    public boolean isKeepConnection() {
        return true;
    }

    @Override
    public String toString() {
        return map.toString();
    }
}
