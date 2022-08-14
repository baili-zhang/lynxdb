package zbl.moonlight.socket.request;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.socket.interfaces.Readable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static zbl.moonlight.core.utils.NumberUtils.*;

public class ReadableSocketRequest extends SocketRequest implements Readable {
    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(INT_LENGTH);
    private final ByteBuffer statusBuffer = ByteBuffer.allocate(BYTE_LENGTH);
    private final ByteBuffer serialBuffer = ByteBuffer.allocate(LONG_LENGTH);
    private ByteBuffer dataBuffer;

    public ReadableSocketRequest() {
    }

    @Override
    public void read() throws IOException {
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        /* 读取长度数据 */
        if(!BufferUtils.isOver(lengthBuffer)) {
            channel.read(lengthBuffer);
            if(!BufferUtils.isOver(lengthBuffer)) {
                return;
            }
            dataBuffer = ByteBuffer.allocate(lengthBuffer.getInt(0) - BYTE_LENGTH - LONG_LENGTH);
        }
        /* 读取状态数据 */
        if(!BufferUtils.isOver(statusBuffer)) {
            channel.read(statusBuffer);
            if(!BufferUtils.isOver(statusBuffer)) {
                return;
            }
            status = statusBuffer.get(0);
        }
        /* 读取序列号 */
        if(!BufferUtils.isOver(serialBuffer)) {
            channel.read(serialBuffer);
            if(!BufferUtils.isOver(serialBuffer)) {
                return;
            }
            serial = serialBuffer.getInt(0);
        }
        /* 读取请求数据 */
        if(!BufferUtils.isOver(dataBuffer)) {
            channel.read(dataBuffer);
            if(BufferUtils.isOver(dataBuffer)) {
                data = dataBuffer.array();
            }
        }
    }

    @Override
    public boolean isReadCompleted() {
        return BufferUtils.isOver(dataBuffer);
    }
}
