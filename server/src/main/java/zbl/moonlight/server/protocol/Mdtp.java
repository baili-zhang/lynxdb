package zbl.moonlight.server.protocol;

import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.response.Response;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class Mdtp {
    public static final int HEADER_LENGTH = 6;

    public static final int READ_ERROR = 1;
    public static final int READ_COMPLETED = 0;
    public static final int READ_COMPLETED_SOCKET_CLOSE = -1;
    public static final int READ_UNCOMPLETED = -2;

    private boolean headerReadCompleted;
    private boolean keyReadCompleted;
    private boolean valueReadCompleted;

    private Integer keyLength;
    private Integer valueLength;

    private byte method;
    private ByteBuffer header;
    private ByteBuffer key;
    private DynamicByteBuffer value;

    private SelectionKey selectionKey;

    private boolean hasResponse;
    private Response response;

    public Mdtp (SelectionKey selectionKey) {
        headerReadCompleted = false;
        keyReadCompleted = false;
        valueReadCompleted = false;

        keyLength = null;
        valueLength = null;

        header = ByteBuffer.allocate(HEADER_LENGTH);
        value = new DynamicByteBuffer();

        this.selectionKey = selectionKey;
    }

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

    /**
     * 从 socketChannel 里读取数据到 data 里
     *
     * @param socketChannel
     * @return READ_COMPLETED 读取完成
     * @return READ_COMPLETED_SOCKET_CLOSE 读取完成，client socket 关闭
     * @return READ_UNCOMPLETED 读取未完成，需要继续读取
     * @throws IOException
     */
    public int read(SocketChannel socketChannel) throws IOException {
        if(!headerReadCompleted) {
            int status = readHeader(socketChannel);

            if(status == READ_UNCOMPLETED || status == READ_ERROR) {
                return status;
            }

            if(status == READ_COMPLETED_SOCKET_CLOSE) {
                return READ_ERROR;
            }

            headerReadCompleted = true;
        }

        if(!keyReadCompleted) {
            int status = readKey(socketChannel);

            if(status == READ_UNCOMPLETED || status == READ_ERROR) {
                return status;
            }

            if(status == READ_COMPLETED_SOCKET_CLOSE) {
                return READ_ERROR;
            }

            keyReadCompleted = true;
        }

        if(!valueReadCompleted) {
            int status = readValue(socketChannel);

            if(status == READ_UNCOMPLETED || status == READ_ERROR) {
                return status;
            }

            if(status == READ_COMPLETED_SOCKET_CLOSE) {
                return READ_COMPLETED_SOCKET_CLOSE;
            }

            valueReadCompleted = true;
        }

        return READ_COMPLETED;
    }

    public byte getMethod() {
        return method;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public DynamicByteBuffer getValue() {
        return value;
    }

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }

    public boolean isHasResponse() {
        return hasResponse;
    }

    public void setHasResponse(boolean hasResponse) {
        this.hasResponse = hasResponse;
    }

    public Response getResponse() {
        return response;
    }

    public void setResponse(Response response) {
        this.response = response;
    }

    private int readHeader(SocketChannel socketChannel) throws IOException {
        while(true) {
            int readLength = socketChannel.read(header);
            if (readLength > 0) continue;

            if(isFull(header)) {
                method = header.get(0);
                keyLength = header.get(1) & 0xff;
                valueLength = ((header.get(2) & 0xff) << 24) |
                        ((header.get(3) & 0xff) << 16) |
                        ((header.get(4) & 0xff) << 8) |
                        (header.get(5) & 0xff);

                key = ByteBuffer.allocate(keyLength);

                return readLength == READ_COMPLETED_SOCKET_CLOSE ? READ_COMPLETED_SOCKET_CLOSE : READ_COMPLETED;
            }

            return readLength == READ_COMPLETED_SOCKET_CLOSE ? READ_ERROR : READ_UNCOMPLETED;
        }
    }

    private int readKey(SocketChannel socketChannel) throws IOException {
        while(true) {
            int readLength = socketChannel.read(key);
            if (readLength > 0) continue;

            if(isFull(key)) {
                return readLength == READ_COMPLETED_SOCKET_CLOSE ? READ_COMPLETED_SOCKET_CLOSE : READ_COMPLETED;
            }

            return readLength == READ_COMPLETED_SOCKET_CLOSE ? READ_ERROR : READ_UNCOMPLETED;
        }
    }

    private int readValue(SocketChannel socketChannel) throws IOException {
        ByteBuffer chunk = value.last();
        while(true) {
            int readLength = socketChannel.read(chunk);
            if (readLength > 0) continue;

            if(value.size() == valueLength) {
                return readLength == READ_COMPLETED_SOCKET_CLOSE ? READ_COMPLETED_SOCKET_CLOSE : READ_COMPLETED;
            }

            if(!isFull(chunk)) {
                return readLength == READ_COMPLETED_SOCKET_CLOSE ? READ_ERROR : READ_UNCOMPLETED;
            }

            chunk = value.last();
        }
    }

    private boolean isFull(ByteBuffer byteBuffer) {
        return byteBuffer.position() == byteBuffer.limit();
    }
}
