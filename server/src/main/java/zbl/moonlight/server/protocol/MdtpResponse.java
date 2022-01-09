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

public class MdtpResponse implements Transportable {
    private static final Logger logger = LogManager.getLogger("MdtpResponse");

    private final int HEADER_LENGTH = 9;

    @Getter
    private final ByteBuffer header;

    @Getter
    @Setter
    private final int identifier;

    @Setter
    @Getter
    private DynamicByteBuffer value;

    @Getter
    private boolean readCompleted;

    @Getter
    private boolean writeCompleted;

    public MdtpResponse(int identifier) {
        header = ByteBuffer.allocate(HEADER_LENGTH);
        writeCompleted = false;
        this.identifier = identifier;
    }

    public void setSuccessNoValue() {
        header.position(0);
        header.put(ResponseCode.SUCCESS_NO_VALUE);
        header.putInt(0);
        header.putInt(identifier);
        header.flip();
    }

    public void setValueExist() {
        header.position(0);
        header.put(ResponseCode.VALUE_EXIST);
        header.putInt(value.getCapacity());
        header.putInt(identifier);
        header.flip();
    }

    public void setValueNotExist() {
        header.position(0);
        header.put(ResponseCode.VALUE_NOT_EXIST);
        header.putInt(0);
        header.putInt(identifier);
        header.flip();
    }

    @Override
    public void read(SocketChannel socketChannel) throws IOException {
        try {
            if(!isOver(header)) {
                socketChannel.read(header);
                if(!isOver(header)) return;;
            }
            if(value == null) {
                readCompleted = true;
                return;
            }
            value.readFrom(socketChannel);
            if(value.isFull()) {
                readCompleted = true;
                value.rewind();
            }
        } catch (IOException e) {
            socketChannel.close();
            logger.info("close SocketChannel when READING.");
            e.printStackTrace();
        }
    }

    public void write(SocketChannel socketChannel) throws IOException {
        logger.info("send response to client.");
        try {
            if(!isOver(header)) {
                socketChannel.write(header);
                if(!isOver(header)) return;
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
}
