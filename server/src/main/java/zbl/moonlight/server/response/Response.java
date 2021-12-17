package zbl.moonlight.server.response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class Response {
    private static final Logger logger = LogManager.getLogger("Response");
    private byte status;
    private DynamicByteBuffer value;

    public Response() {

    }

    public Response(byte status) {
        this.status = status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public void setValue(DynamicByteBuffer value) {
        this.value = value;
    }

    public void write(SocketChannel socketChannel) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        byteBuffer.put(status);
        int length = socketChannel.write(byteBuffer);

        if(value != null) {
            length += socketChannel.write(value.array());
        }

        logger.info("write response to client, data length is {}.", length);
    }
}
