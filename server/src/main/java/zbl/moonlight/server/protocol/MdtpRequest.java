package zbl.moonlight.server.protocol;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class MdtpRequest {
    public static final int HEADER_LENGTH = 6;

    public static final int READ_ERROR = 1;
    public static final int READ_COMPLETED = 0;
    public static final int READ_COMPLETED_SOCKET_CLOSE = -1;
    public static final int READ_UNCOMPLETED = -2;

    private final Logger logger = LogManager.getLogger("MdtpRequest");
    private boolean headerReadCompleted;
    private boolean keyReadCompleted;
    private boolean valueReadCompleted;

    @Getter
    private boolean readCompleted;

    private Integer keyLength;
    private Integer valueLength;

    private byte method;
    private ByteBuffer header;
    private ByteBuffer key;

    @Getter
    @Setter
    private DynamicByteBuffer value;

    public MdtpRequest() {
        headerReadCompleted = false;
        keyReadCompleted = false;
        valueReadCompleted = false;
        readCompleted = false;

        keyLength = null;
        valueLength = null;

        header = ByteBuffer.allocate(HEADER_LENGTH);
        value = null;
    }

    public static ByteBuffer encode(byte code, ByteBuffer key, ByteBuffer value) {
        byte keyLength = (byte) (key.limit() & 0xff);
        int valueLength = value == null ? 0 : value.limit();
        ByteBuffer byteBuffer = ByteBuffer.allocate(HEADER_LENGTH + keyLength + valueLength);
        byteBuffer.put(code).put(keyLength).putInt(valueLength).put(key);
        if(value != null) {
            byteBuffer.put(value);
        }
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
        try {
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

            if(!valueReadCompleted && !valueLength.equals(0)) {
                int status = readValue(socketChannel);

                if(status == READ_UNCOMPLETED || status == READ_ERROR) {
                    return status;
                }

                if(status == READ_COMPLETED_SOCKET_CLOSE) {
                    return READ_COMPLETED_SOCKET_CLOSE;
                }

                valueReadCompleted = true;
            }
        } catch (IOException e) {
            socketChannel.close();
            logger.info("close SocketChannel when READING.");
            e.printStackTrace();
        }

        readCompleted = true;
        return READ_COMPLETED;
    }

    public byte getMethod() {
        return method;
    }

    public ByteBuffer getKey() {
        return key;
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
        if(value == null) {
            value = new DynamicByteBuffer();
        }
        ByteBuffer chunk = value.last();
        while(true) {
            int readLength = socketChannel.read(chunk);
            if (readLength > 0) continue;

            if(value.writtenSize() == valueLength) {
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

    @Override
    public String toString() {
        return "method: " + MdtpMethod.getMethodName(method) + ", key: " +
                new String(key.array()) + ", value length is:" + (value == null ? 0 : value.size());
    }
}
