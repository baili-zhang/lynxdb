package zbl.moonlight.server.protocol;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class MdtpResponse {
    private final Logger logger = LogManager.getLogger("MdtpResponse");

    @Getter
    private ByteBuffer header;
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
    }

    public void write(SocketChannel socketChannel) throws IOException {
        header.flip();
        writeLength += socketChannel.write(header);
        if(writeLength == 5) {
            writeCompleted = true;
        }
        logger.info("write length is: " + writeLength);
    }
}
