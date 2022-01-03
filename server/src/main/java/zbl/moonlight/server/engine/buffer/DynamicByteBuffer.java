package zbl.moonlight.server.engine.buffer;

import lombok.Getter;
import lombok.ToString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

@ToString
public class DynamicByteBuffer {
    @Getter
    private final List<ByteBuffer> bufferList = new ArrayList<>();
    @Getter
    private final int capacity;

    public DynamicByteBuffer(int capacity) {
        this.capacity = capacity;
        ByteBuffer byteBuffer = ByteBuffer.allocate(capacity);
        bufferList.add(byteBuffer);
    }

    public DynamicByteBuffer(ByteBuffer buffer) {
        this.capacity = buffer.limit();
        bufferList.add(buffer);
    }

    public void flip () {
        for(ByteBuffer byteBuffer : bufferList) {
            byteBuffer.flip();
        }
    }

    public void rewind() {
        for(ByteBuffer byteBuffer : bufferList) {
            byteBuffer.rewind();
        }
    }

    public boolean isFull() {
        ByteBuffer byteBuffer = bufferList.get(bufferList.size() - 1);
        return byteBuffer.position() == byteBuffer.limit();
    }

    public void writeTo(SocketChannel socketChannel) throws IOException {
        for(ByteBuffer byteBuffer : bufferList) {
            if(byteBuffer.position() == byteBuffer.limit()) {
                continue;
            }
            socketChannel.write(byteBuffer);
            if(byteBuffer.position() != byteBuffer.limit()) {
                break;
            }
        }
    }

    public void readFrom(SocketChannel socketChannel) throws IOException {
        for (ByteBuffer byteBuffer : bufferList) {
            if(byteBuffer.position() == byteBuffer.limit()) {
                continue;
            }
            socketChannel.read(byteBuffer);
            if(byteBuffer.position() != byteBuffer.limit()) {
                break;
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for(ByteBuffer byteBuffer : bufferList) {
            stringBuilder.append(new String(byteBuffer.array()));
        }
        return stringBuilder.toString();
    }
}
