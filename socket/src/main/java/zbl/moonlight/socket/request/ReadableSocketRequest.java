package zbl.moonlight.socket.request;

import lombok.Getter;
import zbl.moonlight.core.enhance.EnhanceByteBuffer;
import zbl.moonlight.socket.interfaces.Readable;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ReadableSocketRequest extends SocketRequest implements Readable {
    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH);
    private final ByteBuffer statusBuffer = ByteBuffer.allocate(NumberUtils.BYTE_LENGTH);
    private final ByteBuffer serialBuffer = ByteBuffer.allocate(NumberUtils.LONG_LENGTH);
    @Getter
    private ByteBuffer bytes;

    public ReadableSocketRequest(SelectionKey selectionKey) {
        super(selectionKey);
    }

    @Override
    public void read() throws IOException {
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        /* 读取长度数据 */
        if(!EnhanceByteBuffer.isOver(lengthBuffer)) {
            channel.read(lengthBuffer);
            if(!EnhanceByteBuffer.isOver(lengthBuffer)) {
                return;
            }
            bytes = ByteBuffer.allocate(lengthBuffer.getInt(0));
        }
        /* 读取状态数据 */
        if(!EnhanceByteBuffer.isOver(statusBuffer)) {
            channel.read(statusBuffer);
            if(!EnhanceByteBuffer.isOver(statusBuffer)) {
                return;
            }
        }
        /* 读取序列号 */
        if(!EnhanceByteBuffer.isOver(serialBuffer)) {
            channel.read(serialBuffer);
            if(!EnhanceByteBuffer.isOver(serialBuffer)) {
                return;
            }
        }
        /* 读取请求数据 */
        if(!EnhanceByteBuffer.isOver(bytes)) {
            channel.read(bytes);
        }
    }

    @Override
    public boolean isReadCompleted() {
        return EnhanceByteBuffer.isOver(bytes);
    }
}
