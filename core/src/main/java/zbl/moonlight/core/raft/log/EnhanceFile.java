package zbl.moonlight.core.raft.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

public class EnhanceFile {
    private static final Logger logger = LogManager.getLogger("EnhanceFile");

    private final FileChannel input;
    private final FileChannel output;

    public EnhanceFile(String filePath) throws IOException {
        Path path = Path.of(filePath);
        File file = path.toFile();
        if(file.createNewFile()) {
            logger.info("Create new file: {}", filePath);
        }

        FileInputStream inputStream = new FileInputStream(file);
        FileOutputStream outputStream = new FileOutputStream(file, true);

        input = inputStream.getChannel();
        output = outputStream.getChannel();
    }

    public void read(ByteBuffer dst, long position) throws IOException {
        input.read(dst, position);
    }

    public void write(ByteBuffer src, long position) throws IOException {
        output.write(src, position);
    }
}
