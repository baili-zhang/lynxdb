package zbl.moonlight.core.socket.request;

import lombok.Getter;
import zbl.moonlight.core.socket.interfaces.Readable;
import zbl.moonlight.core.socket.interfaces.SocketState;
import zbl.moonlight.core.utils.ByteBufferUtils;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ReadableSocketRequest implements Readable {
    private final SelectionKey key;
    private final ByteBuffer length = ByteBuffer.allocate(NumberUtils.INT_LENGTH);
    private final ByteBuffer status = ByteBuffer.allocate(NumberUtils.BYTE_LENGTH);
    @Getter
    private ByteBuffer data;
    private boolean isKeepConnection = true;

    public ReadableSocketRequest(SelectionKey selectionKey) {
        key = selectionKey;
    }

    public SelectionKey selectionKey() {
        return key;
    }

    @Override
    public void read() throws IOException {
        SocketChannel channel = (SocketChannel) key.channel();
        /* 读取长度数据 */
        if(!ByteBufferUtils.isOver(length)) {
            channel.read(length);
            if(!ByteBufferUtils.isOver(length)) {
                return;
            }
            data = ByteBuffer.allocate(length.getInt(0));
        }
        /* 读取状态数据 */
        if(!ByteBufferUtils.isOver(status)) {
            channel.read(status);
            if(!ByteBufferUtils.isOver(status)) {
                return;
            }
            isKeepConnection = SocketState.isStayConnected(status.get(0));
        }
        /* 读取请求数据 */
        if(!ByteBufferUtils.isOver(data)) {
            channel.read(data);
        }
    }

    @Override
    public boolean isReadCompleted() {
        return ByteBufferUtils.isOver(data);
    }

    public boolean isKeepConnection() {
        return isKeepConnection;
    }

    public SocketRequest socketRequest() {
        byte socketStatus = status.get(0);

        if(!isReadCompleted()) {
            throw new RuntimeException("Can not get socket request before read completed.");
        }

        if(SocketState.isBroadcast(socketStatus)) {
            return new SocketRequest(true, socketStatus, data.array(), null, key);
        } else {
            return new SocketRequest(false, socketStatus, data.array(), null, key);
        }
    }
}
