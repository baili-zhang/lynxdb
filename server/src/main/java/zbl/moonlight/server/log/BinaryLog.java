package zbl.moonlight.server.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.exception.IncompleteBinaryLogException;
import zbl.moonlight.server.protocol.MdtpRequest;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BinaryLog {
    private static final Logger logger = LogManager.getLogger("BinaryLog");

    private final int HEADER_LIMIT = 6;
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
        ByteBuffer header = request.getHeader();
        header.limit(6);
        ByteBuffer key = request.getKey();
        ByteBuffer value = request.getValue();

        header.rewind();
        key.rewind();
        if(value != null) {
            value.rewind();
        }

        try {
            append(request.getHeader());
            append(request.getKey());
            if(value != null) {
                append(value);
            }
            logger.debug("Write a entry to log file.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<MdtpRequest> read() throws IOException, IncompleteBinaryLogException {
        List<MdtpRequest> requests = new ArrayList<>();
        FileChannel channel = inputStream.getChannel();
        long readLength;
        while (true) {
            MdtpRequest request = new MdtpRequest();

            ByteBuffer header = request.getHeader();
            header.limit(HEADER_LIMIT);
            readLength = channel.read(header, position);
            /* 读取到文件末尾 */
            if(readLength == -1) {
                logger.debug("Read {} requests from log file.", requests.size());
                return requests;
            }
            position += readLength;
            header.limit(header.capacity());
            /* 设置序列号，TODO:二进制日志文件格式要重新定义 */
            header.putInt(0);
            request.parseHeader();

            readLength = channel.read(request.getKey(), position);
            if(readLength == -1) {
                throw new IncompleteBinaryLogException("Incomplete binary log file");
            }
            position += readLength;

            ByteBuffer value = request.getValue();
            if(value == null) {
                requests.add(request);
                continue;
            }

            readLength = channel.read(value, position);
            position += readLength;

            requests.add(request);
        }
    }

    private void append(ByteBuffer byteBuffer) throws IOException {
        FileChannel channel = outputStream.getChannel();
        position += channel.write(byteBuffer, position);
    }
}
