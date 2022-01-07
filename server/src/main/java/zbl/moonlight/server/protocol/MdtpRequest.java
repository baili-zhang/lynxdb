package zbl.moonlight.server.protocol;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static zbl.moonlight.server.utils.ByteBufferUtils.isOver;

public class MdtpRequest implements Transportable {
    public static final int HEADER_LENGTH = 10;
    private final Logger logger = LogManager.getLogger("MdtpRequest");

    private Integer keyLength;
    private Integer valueLength;

    @Getter
    private int identifier;

    @Getter
    private byte method;
    @Getter
    private ByteBuffer header;

    @Getter
    private ByteBuffer key;

    @Getter
    @Setter
    private DynamicByteBuffer value;

    @Getter
    private boolean readCompleted;

    @Getter
    private boolean writeCompleted;

    public MdtpRequest() {
        readCompleted = false;

        keyLength = null;
        valueLength = null;

        header = ByteBuffer.allocate(HEADER_LENGTH);
        value = null;
    }

    public void parseHeader() {
        method = header.get(0);
        keyLength = header.get(1) & 0xff;
        valueLength = header.getInt(2);
        identifier = header.getInt(6);

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
            if(!isOver(header)) {
                readHeader(socketChannel);
                if(!isOver(header)) {
                    return;
                }
            }

            if(!isOver(key)) {
                readKey(socketChannel);
                if(!isOver(key)) {
                    return;
                }
            }

            if(!valueLength.equals(0) && !value.isFull()) {
                readValue(socketChannel);
            }
        } catch (IOException e) {
            socketChannel.close();
            logger.info("close SocketChannel when READING.");
            e.printStackTrace();
        }

        readCompleted = valueLength == null || valueLength.equals(0) ? isOver(key) : value.isFull();
    }

    @Override
    public void write(SocketChannel socketChannel) throws IOException {
        try {
            if(!isOver(header)) {
                socketChannel.write(header);
                if(!isOver(header)) {
                    return;
                }
            }

            if(!isOver(key)) {
                socketChannel.write(key);
                if(!isOver(key)) {
                    return;
                }
            }
            if(!valueLength.equals(0) && !value.isFull()) {
                value.writeTo(socketChannel);
            }
        } catch (IOException e) {
            socketChannel.close();
            writeCompleted = true;
            logger.info("close SocketChannel when WRITING.");
            e.printStackTrace();
        }

        writeCompleted = valueLength.equals(0) ? isOver(key) : value.isFull();
    }

    private void readHeader(SocketChannel socketChannel) throws IOException {
        while(true) {
            logger.info("read mdtp request header");
            int readLength = socketChannel.read(header);
            if (readLength > 0) continue;

            if(isOver(header)) {
                parseHeader();
                break;
            }
        }
    }

    @Override
    public String toString() {
        return "method: " + MdtpMethod.getMethodName(method) + ", key: " + new String(key.array()) + ", value: " + value;
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
}
