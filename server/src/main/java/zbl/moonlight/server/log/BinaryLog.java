package zbl.moonlight.server.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.exception.IncompleteBinaryLogException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class BinaryLog {
    private static final Logger logger = LogManager.getLogger("BinaryLog");

    private static final String FILENAME = "binlog";
    private static final String FILE_EXTENSION = ".log";
    private static final String DEFAULT_FOLDER = "/logs";

    private final FileOutputStream outputStream;
    private final FileInputStream inputStream;

    private long position = 0;

    public BinaryLog() throws IOException {
        Path path = Path.of(System.getProperty("user.dir"), DEFAULT_FOLDER, FILENAME + FILE_EXTENSION);
        File file = path.toFile();
        if (!file.exists()) {
            /* TODO:如果没有logs目录会出现“找不到系统路径的异常” */
            file.createNewFile();
        }
        inputStream = new FileInputStream(file);
        outputStream = new FileOutputStream(file, true);
    }

    private void append(ByteBuffer byteBuffer) throws IOException {
        FileChannel channel = outputStream.getChannel();
        position += channel.write(byteBuffer, position);
    }
}
