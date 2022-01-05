package zbl.moonlight.server.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.exception.IncompleteBinaryLogException;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.MdtpRequest;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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

    /* TODO:会不会出现MdtpRequest没写完的情况 */
    public void write(MdtpRequest request) {
        byte method = request.getMethod();
        if(method != MdtpMethod.SET && method != MdtpMethod.DELETE) {
            return;
        }

        ByteBuffer header = request.getHeader();
        ByteBuffer key = request.getKey();
        DynamicByteBuffer value = request.getValue();

        header.rewind();
        key.rewind();

        logger.info("write to binary log, request is: " + request);
        if(value != null) {
            value.rewind();
        }

        try {
            append(request.getHeader());
            append(request.getKey());
            if(value != null) {
                for(ByteBuffer buffer : value.getBufferList()) {
                    append(buffer);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(value != null) {
            value.rewind();
        }
    }

    public List<MdtpRequest> read() throws IOException, IncompleteBinaryLogException {
        List<MdtpRequest> requests = new ArrayList<>();
        FileChannel channel = inputStream.getChannel();
        int readLength;
        while (true) {
            MdtpRequest request = new MdtpRequest();

            ByteBuffer header = request.getHeader();
            readLength = channel.read(header, position);
            if(readLength == -1) {
                return requests;
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
                requests.add(request);
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
            requests.add(request);
        }
        return requests;
    }

    private void append(ByteBuffer byteBuffer) throws IOException {
        FileChannel channel = outputStream.getChannel();
        position += channel.write(byteBuffer, position);
    }
}
