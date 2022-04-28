package zbl.moonlight.core.raft.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class EnhanceFile {
    private static final Logger logger = LogManager.getLogger("EnhanceFile");

    private final File file;

    private final FileChannel input;
    private final FileChannel output;

    private final FileInputStream inputStream;
    private final FileOutputStream outputStream;

    public EnhanceFile(String dirname, String filename) throws IOException {
        Path dirPath = Path.of(dirname);
        Files.createDirectories(dirPath);

        Path filePath = Path.of(dirname, filename);
        file = filePath.toFile();
        if(!file.exists() && file.createNewFile()) {
            logger.info("Create new file: {}", filePath);
        }

        inputStream = new FileInputStream(file);
        outputStream = new FileOutputStream(file, true);

        input = inputStream.getChannel();
        output = outputStream.getChannel();
    }

    public void read(ByteBuffer dst, long position) throws IOException {
        input.read(dst, position);
    }

    public void write(ByteBuffer src, long position) throws IOException {
        output.write(src, position);
    }

    public boolean delete() throws IOException {
        inputStream.close();
        outputStream.close();
        return file.delete();
    }
}
