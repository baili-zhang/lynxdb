package zbl.moonlight.core.enhance;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

        input = new FileInputStream(file).getChannel();
        output = new FileOutputStream(file, true).getChannel();
    }

    public void read(ByteBuffer dst, long position) throws IOException {
        input.read(dst, position);
    }

    public ByteBuffer read(long position, int length) throws IOException {
        ByteBuffer dst = ByteBuffer.allocate(length);
        input.read(dst, position);
        return dst;
    }

    public void write(ByteBuffer src, long position) throws IOException {
        output.write(src, position);
    }

    public boolean delete() throws IOException {
        close();
        return file.delete();
    }

    public long length() {
        return file.length();
    }

    public int readInt(long position) throws IOException {
        ByteBuffer buffer = EnhanceByteBuffer.intByteBuffer();
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
        return new String(readBytes(position));
    }

    public byte[] readBytes(long position) throws IOException {
        ByteBuffer length = EnhanceByteBuffer.intByteBuffer();
        read(length, position);
        int len = length.rewind().getInt();
        ByteBuffer content = ByteBuffer.allocate(len);
        long contentPosition = position + NumberUtils.INT_LENGTH;
        read(content, contentPosition);
        return content.array();
    }

    public void writeInt(int src, long position) throws IOException {
        ByteBuffer buffer = EnhanceByteBuffer.intByteBuffer();
        buffer.putInt(src).rewind();
        write(buffer, position);
    }

    public void writeByte(byte src, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.BYTE_LENGTH);
        buffer.put(src).rewind();
        write(buffer, position);
    }

    public long writeString(String src, long position) throws IOException {
        byte[] srcBytes = src.getBytes(StandardCharsets.UTF_8);

        ByteBuffer length = EnhanceByteBuffer.intByteBuffer();
        length.putInt(srcBytes.length).rewind();
        write(length, position);

        long srcPosition = position + NumberUtils.INT_LENGTH;
        write(ByteBuffer.wrap(srcBytes), srcPosition);

        return position + NumberUtils.INT_LENGTH + srcBytes.length;
    }

    public void append(byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        output.write(buffer, file.length());
    }

    public void close() throws IOException {
        input.close();
        output.close();
    }

    @Override
    public String toString() {
        return file.getPath();
    }
}
