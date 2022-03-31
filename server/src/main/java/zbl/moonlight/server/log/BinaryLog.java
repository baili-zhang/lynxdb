package zbl.moonlight.server.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.core.utils.ByteBufferUtils;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class BinaryLog {
    private static final Logger logger = LogManager.getLogger("BinaryLog");

    private static final Configuration config = MdtpServerContext.getInstance().getConfiguration();
    private static final String FILENAME = config.getHost() + "-" + config.getPort() + "-" + "binlog";
    private static final String FILE_EXTENSION = ".log";
    private static final String DEFAULT_FOLDER = "/logs";

    private final FileOutputStream outputStream;
    private final FileInputStream inputStream;

    private long writePosition = 0;
    private long readPosition = 0;

    public BinaryLog() throws IOException {
        Path dir = Path.of(System.getProperty("user.dir"), DEFAULT_FOLDER);
        if(!dir.toFile().exists()) {
            Files.createDirectories(dir);
        }

        Path path = Path.of(System.getProperty("user.dir"),
                DEFAULT_FOLDER, FILENAME + FILE_EXTENSION);
        File file = path.toFile();
        if (!file.exists()) {
            file.createNewFile();
        }
        inputStream = new FileInputStream(file);
        outputStream = new FileOutputStream(file, true);
    }

    private void append(ByteBuffer byteBuffer) throws IOException {
        FileChannel channel = outputStream.getChannel();
        writePosition += channel.write(byteBuffer, writePosition);
    }

    private void read(ByteBuffer byteBuffer) throws IOException {
        /* TODO:校验是否读取越界 */
        FileChannel channel = inputStream.getChannel();
        readPosition += channel.read(byteBuffer, readPosition);
    }

    private ByteBuffer readEntry() throws IOException {
        ByteBuffer lengthByteBuffer = ByteBufferUtils.intByteBuffer();
        read(lengthByteBuffer);
        int length = ByteArrayUtils.toInt(lengthByteBuffer.array());
        ByteBuffer entry = ByteBuffer.allocate(length);
        read(entry);
        return entry;
    }
}
