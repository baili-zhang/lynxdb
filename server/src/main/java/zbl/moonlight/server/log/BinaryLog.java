package zbl.moonlight.server.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.exception.IncompleteBinaryLogException;
import zbl.moonlight.server.protocol.MdtpRequest;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class BinaryLog {
    private final static Logger logger = LogManager.getLogger("BinaryLog");
    private final String FILENAME = "binlog";
    private final String FILE_EXTENSION = ".log";
    private final String DEFAULT_FOLDER = "/logs";
    private final FileOutputStream outputStream;
    private final FileInputStream inputStream;

    private long position = 0;

    public BinaryLog() throws IOException {
        Path path = Path.of(System.getProperty("user.dir"), DEFAULT_FOLDER, FILENAME + FILE_EXTENSION);
        File file = path.toFile();
        if (!file.exists()) {
            file.createNewFile();
        }
        inputStream = new FileInputStream(file);
        outputStream = new FileOutputStream(file, true);
    }

    public void append(ByteBuffer byteBuffer) throws IOException {
        FileChannel channel = outputStream.getChannel();
        position += channel.write(byteBuffer, position);
    }

    public void read() throws IOException, IncompleteBinaryLogException {
        FileChannel channel = inputStream.getChannel();
        Engine engine = ServerContext.getInstance().getEngine();
        int readLength;
        while (true) {
            MdtpRequest request = new MdtpRequest();

            ByteBuffer header = request.getHeader();
            readLength = channel.read(header, position);
            if(readLength == -1) {
                return;
            }
            position += readLength;
            request.parseHeader();

            readLength = channel.read(request.getKey(), position);
            if(readLength == -1) {
                throw new IncompleteBinaryLogException("Incomplete binary log file");
            }
            position += readLength;

            DynamicByteBuffer value = request.getValue();
            if(value == null) {
                logger.info("load data from binary log, request is: " + request);
                engine.exec(request);
                break;
            }
            for(ByteBuffer byteBuffer : value.getBufferList()) {
                readLength = channel.read(byteBuffer, position);
                if(readLength == -1) {
                    throw new IncompleteBinaryLogException("Incomplete binary log file");
                }
                position += readLength;
            }
            value.rewind();
            logger.info("load data from binary log, request is: " + request);
            engine.exec(request);
        }
    }
}
