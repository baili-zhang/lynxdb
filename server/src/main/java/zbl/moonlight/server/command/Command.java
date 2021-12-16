package zbl.moonlight.server.command;

import lombok.Data;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

@Data
public class Command implements Cloneable {
    private static final int HEADER_LENGTH = 6;

    private byte code;
    private SelectionKey selectionKey;
    private int keyLength;
    private ByteBuffer key;
    private long valueLength;
    private ByteBuffer value;
    private ByteBuffer response;

    /**
     * Is Command read completed.
     */
    private boolean readCompleted;

    /**
     * should send response to client or not.
     */
    private boolean sendResponse;
    private DynamicByteBuffer data;

    public static ByteBuffer encode(byte code, ByteBuffer key, ByteBuffer value) throws EncodeException {
        if(key.limit() > 0xff) {
            throw new EncodeException("key length cannot exceed 0xff.");
        }
        byte keyLength = (byte) (key.limit() & 0xff);
        int valueLength = value.limit();
        ByteBuffer byteBuffer = ByteBuffer.allocate(HEADER_LENGTH + keyLength + valueLength);
        byteBuffer.put(code).put(keyLength).putInt(valueLength).put(key).put(value);
        return byteBuffer;
    }

    public Command() {
        readCompleted = false;
        data = new DynamicByteBuffer(10);
    }

    public boolean isValid() {
        if(data.size() <= 6) {
            return false;
        }

        ByteBuffer first = data.getFirst();
        int keyLength = first.get(1) & 0xff;
        int valueLength = ((first.get(2) & 0xff) << 24) |
                ((first.get(3) & 0xff) << 16) |
                ((first.get(4) & 0xff) << 8) |
                (first.get(5) & 0xff);

        return data.size() == 6 + keyLength + valueLength;
    }

    public void decode () throws DecodeException {
        if(!isValid()) {
            throw new DecodeException("Invalid Command.");
        }

        // ...
    }

    public int read(SocketChannel socketChannel) throws IOException {
        ByteBuffer byteBuffer = data.last();
        while(true) {
            int readLength = socketChannel.read(byteBuffer);

            if (readLength == -1) return -1;
            if (readLength > 0) continue;

            if(data.isFull()) {
                byteBuffer = data.last();
                continue;
            }

            return data.size();
        }
    }
}
