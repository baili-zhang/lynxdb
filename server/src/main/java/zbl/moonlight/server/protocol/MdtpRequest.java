package zbl.moonlight.server.protocol;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static zbl.moonlight.server.utils.ByteBufferUtils.isOver;

public class MdtpRequest implements Transportable {
    public static final int HEADER_LENGTH = 10;
    private static final Logger logger = LogManager.getLogger("MdtpRequest");

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
    private ByteBuffer value;

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

    public MdtpRequest(int identifier, byte method,
                       ByteBuffer header, ByteBuffer key, ByteBuffer value) {
        this.identifier = identifier;
        this.method = method;
        this.header = header.asReadOnlyBuffer();
        this.key = key.asReadOnlyBuffer();
        if(value != null) {
            this.value = value.asReadOnlyBuffer();
        }
    }

    public final MdtpRequest duplicate() {
        return new MdtpRequest(identifier, method, header, key, value);
    }

    public final void parseHeader() {
        method = header.get(0);
        keyLength = header.get(1) & 0xff;
        valueLength = header.getInt(2);
        identifier = header.getInt(6);

        key = ByteBuffer.allocateDirect(keyLength);
        if(!valueLength.equals(0)) {
            value = ByteBuffer.allocateDirect(valueLength);
        }
    }

    public final void read(SocketChannel socketChannel) throws IOException {
        try {
            if(!isOver(header)) {
                readHeader(socketChannel);
                if(!isOver(header)) {
                    return;
                } else {
                    parseHeader();
                }
            }

            if(keyLength.equals(0)) {
                readCompleted = true;
                return;
            }

            if(!isOver(key)) {
                readKey(socketChannel);
                if(!isOver(key)) {
                    return;
                }
            }

            if(!valueLength.equals(0) && !isOver(value)) {
                readValue(socketChannel);
            }
        } catch (IOException e) {
            socketChannel.close();
            logger.info("close SocketChannel when READING.");
            e.printStackTrace();
        }

        readCompleted = valueLength == null || valueLength.equals(0) ? isOver(key) : isOver(value);
    }

    @Override
    public final void write(SocketChannel socketChannel) throws IOException {
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
            if(!valueLength.equals(0) && !isOver(value)) {
                socketChannel.write(value);
            }
        } catch (IOException e) {
            socketChannel.close();
            writeCompleted = true;
            logger.info("close SocketChannel when WRITING.");
            e.printStackTrace();
        }

        writeCompleted = valueLength.equals(0) ? isOver(key) : isOver(value);
    }

    private void readHeader(SocketChannel socketChannel) throws IOException {
        socketChannel.read(header);
    }

    private void readKey(SocketChannel socketChannel) throws IOException {
        socketChannel.read(key);
    }

    private void readValue(SocketChannel socketChannel) throws IOException {
        socketChannel.read(value);
    }
}
