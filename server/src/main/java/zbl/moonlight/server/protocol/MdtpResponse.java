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

    @Getter
    private boolean writeCompleted;

    public MdtpResponse() {
        header = ByteBuffer.allocate(HEADER_LENGTH);
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
        header.putInt(value.getCapacity());
        header.flip();
    }

    public void setValueNotExist() {
        header.position(0);
        header.put(ResponseCode.VALUE_NOT_EXIST);
        header.putInt(0);
        header.flip();
    }

    public void write(SocketChannel socketChannel) throws IOException {
        logger.info("send response to client.");
        try {
            if(!isFull(header)) {
                socketChannel.write(header);
                if(!isFull(header)) return;
            }
            if(value == null) {
                writeCompleted = true;
                return;
            }
            value.writeTo(socketChannel);
            if(value.isFull()) {
                writeCompleted = true;
                value.rewind();
            }
        } catch (IOException e) {
            socketChannel.close();
            writeCompleted = true;
            logger.info("close SocketChannel when WRITING.");
            e.printStackTrace();
        }
    }

    private boolean isFull(ByteBuffer byteBuffer) {
        return byteBuffer.position() == byteBuffer.limit();
    }
}
