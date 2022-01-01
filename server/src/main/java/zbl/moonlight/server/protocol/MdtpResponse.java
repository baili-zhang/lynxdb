package zbl.moonlight.server.protocol;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class MdtpResponse {
    private final Logger logger = LogManager.getLogger("MdtpResponse");

    private final int HEADER_LENGTH = 5;

    @Getter
    private ByteBuffer header;

    @Setter
    @Getter
    private DynamicByteBuffer value;

    /**
     * 已经写回的长度
     */
    private int writeLength;

    @Getter
    private boolean writeCompleted;

    public MdtpResponse() {
        header = ByteBuffer.allocate(5);
        writeLength = 0;
        writeCompleted = false;
    }

    public void setSuccessNoValue() {
        header.position(0);
        header.put(ResponseCode.SUCCESS_NO_VALUE);
        header.putInt(0);
        header.flip();
    }

    public void setValueExist() {
        header.position(0);
        header.put(ResponseCode.VALUE_EXIST);
        header.putInt(value.size());
        header.flip();
    }

    public void setValueNotExist() {
        header.position(0);
        header.put(ResponseCode.VALUE_NOT_EXIST);
        header.putInt(0);
        header.flip();
    }

    /**
     * (1) 响应头没全部写入
     * (2) value 没有全部写入
     * (2) 写入长度超过（响应头长度 + value长度），不应该发生
     * @param socketChannel
     * @throws IOException
     */
    public void write(SocketChannel socketChannel) throws IOException {
        try {
            if(writeLength < HEADER_LENGTH) {
                writeLength += socketChannel.write(header);
            }
            if(writeLength == HEADER_LENGTH && value == null) {
                writeCompleted = true;
                logger.info("write length is: " + writeLength);
                return;
            }
            writeLength += value.writeTo(socketChannel);
        } catch (IOException e) {
            socketChannel.close();
            writeCompleted = true;
            logger.info("close SocketChannel when WRITING.");
            e.printStackTrace();
        }
        if(writeLength == HEADER_LENGTH + value.writtenSize()) {
            writeCompleted = true;
            logger.info("write length is: " + writeLength);
        }

        if(writeLength > HEADER_LENGTH + value.writtenSize()) {
            throw new IOException("writeLength > HEADER_LENGTH + value.size()");
        }
    }
}
