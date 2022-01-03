package zbl.moonlight.server.log;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class Log {
    private final String FILENAME = "binlog";
    private final String FILE_EXTENSION = ".log";
    private final String DEFAULT_FOLDER = "/logs";
    private final FileChannel channel;

    private long position = 0;

    public Log() throws IOException {
        Path path = Path.of(System.getProperty("user.dir"), DEFAULT_FOLDER, FILENAME + FILE_EXTENSION);
        File file = path.toFile();
        if (!file.exists()) {
            file.createNewFile();
        }
        FileOutputStream outputStream = new FileOutputStream(file);
        channel = outputStream.getChannel();
    }

    public void append(ByteBuffer byteBuffer) throws IOException {
        position += channel.write(byteBuffer, position);
    }
}
