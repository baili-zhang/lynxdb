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
    private final Logger logger = LogManager.getLogger("MdtpRequest");

    @Getter
    private boolean readCompleted;

    private Integer keyLength;
    private Integer valueLength;

    @Getter
    private byte method;
    @Getter
    private ByteBuffer header;

    @Getter
    private ByteBuffer key;

    @Getter
    @Setter
    private DynamicByteBuffer value;

    public MdtpRequest() {
        readCompleted = false;

        keyLength = null;
        valueLength = null;

        header = ByteBuffer.allocate(HEADER_LENGTH);
        value = null;
    }

    public MdtpRequest(byte method, ByteBuffer key, ByteBuffer value) {
        this.method = method;
        this.key = key;
        this.value = new DynamicByteBuffer(value);
    }

    public void parseHeader() {
        method = header.get(0);
        keyLength = header.get(1) & 0xff;
        valueLength = ((header.get(2) & 0xff) << 24) |
                ((header.get(3) & 0xff) << 16) |
                ((header.get(4) & 0xff) << 8) |
                (header.get(5) & 0xff);

        key = ByteBuffer.allocate(keyLength);
        if(!valueLength.equals(0)) {
            value = new DynamicByteBuffer(valueLength);
        }
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
    public void read(SocketChannel socketChannel) throws IOException {
        try {
            if(!isFull(header)) {
                readHeader(socketChannel);
            }

            if(!isFull(key)) {
                readKey(socketChannel);
            }

            if(!valueLength.equals(0) && !value.isFull()) {
                readValue(socketChannel);
            }
        } catch (IOException e) {
            socketChannel.close();
            logger.info("close SocketChannel when READING.");
            e.printStackTrace();
        }

        readCompleted = valueLength.equals(0) ? isFull(key) : value.isFull();
    }

    private void readHeader(SocketChannel socketChannel) throws IOException {
        while(true) {
            logger.info("read mdtp request header");
            int readLength = socketChannel.read(header);
            if (readLength > 0) continue;

            if(isFull(header)) {
                parseHeader();
                break;
            }
        }
    }

    private void readKey(SocketChannel socketChannel) throws IOException {
        while(true) {
            logger.info("read mdtp request key");
            int readLength = socketChannel.read(key);
            if (readLength <= 0) break;
        }
    }

    private void readValue(SocketChannel socketChannel) throws IOException {
        if(value == null) {
            value = new DynamicByteBuffer(valueLength);
        }
        value.readFrom(socketChannel);
    }

    private boolean isFull(ByteBuffer byteBuffer) {
        return byteBuffer.position() == byteBuffer.limit();
    }

    @Override
    public String toString() {
        return "method: " + MdtpMethod.getMethodName(method) + ", key: " + new String(key.array()) + ", value: " + value;
    }
}
