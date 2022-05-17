package zbl.moonlight.core.enhance;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.utils.ByteBufferUtils;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
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
        } else {
            logger.info("File existed: {}", filePath);
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

    public long length() {
        return file.length();
    }

    public int readInt(long position) throws IOException {
        ByteBuffer buffer = ByteBufferUtils.intByteBuffer();
        read(buffer, position);
        return buffer.rewind().getInt();
    }

    public byte readByte(long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.BYTE_LENGTH);
        read(buffer, position);
        return buffer.rewind().get();
    }

    /**
     * 读取一段不定长的字符串
     * 数据结构：[字符串长度][字符串内容]
     * @return 字符串
     * @throws IOException IO异常
     */
    public String readString(long position) throws IOException {
        ByteBuffer length = ByteBufferUtils.intByteBuffer();
        read(length, position);
        int len = length.rewind().getInt();
        ByteBuffer content = ByteBuffer.allocate(len);
        long contentPosition = position + NumberUtils.INT_LENGTH;
        read(content, contentPosition);
        return new String(content.array());
    }

    public long writeInt(int src, long position) throws IOException {
        ByteBuffer buffer = ByteBufferUtils.intByteBuffer();
        buffer.putInt(src).rewind();
        write(buffer, position);
        return position + NumberUtils.INT_LENGTH;
    }

    public long writeByte(byte src, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.BYTE_LENGTH);
        buffer.put(src).rewind();
        write(buffer, position);
        return position + NumberUtils.BYTE_LENGTH;
    }

    public long writeString(String src, long position) throws IOException {
        byte[] srcBytes = src.getBytes(StandardCharsets.UTF_8);

        ByteBuffer length = ByteBufferUtils.intByteBuffer();
        length.putInt(srcBytes.length).rewind();
        write(length, position);

        long srcPosition = position + NumberUtils.INT_LENGTH;
        write(ByteBuffer.wrap(srcBytes), srcPosition);

        return position + NumberUtils.INT_LENGTH + srcBytes.length;
    }
}
