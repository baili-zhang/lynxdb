package com.bailizhang.lynxdb.core.file;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.NumberUtils.LONG_LENGTH;

public class LogFile implements AutoCloseable {
    private static final int DEFAULT_FILE_SIZE = 4 * 1024 * 1024;

    private final FileChannel channel;

    public LogFile(String dirname, String filename) throws IOException {
        Path path = Path.of(dirname, filename);
        channel = FileChannel.open(path);
    }


    public void close() throws IOException {
        channel.close();
    }

    public void append(BytesListConvertible convertible) throws IOException {
        BytesList bytesList = convertible.toBytesList();
        byte[] entry = bytesList.toBytes();

        CRC32C crc32C = new CRC32C();
        crc32C.update(entry);
        long crc32CValue = crc32C.getValue();

        ByteBuffer crc32CBuffer = BufferUtils.longByteBuffer(crc32CValue);
        ByteBuffer entryBuffer = ByteBuffer.wrap(entry);
        int length = crc32CBuffer.capacity() + entryBuffer.capacity();
        ByteBuffer lengthBuffer = BufferUtils.intByteBuffer(length);

        ByteBuffer[] buffers = new ByteBuffer[]{lengthBuffer, crc32CBuffer, entryBuffer};
        channel.write(buffers);
        channel.force(false);
    }

    public byte[] readEntry(int i) throws IOException {
        int p = 0;
        while(p <= i) {
            ByteBuffer lengthBuffer = BufferUtils.intByteBuffer();
            int length = lengthBuffer.getInt();

            int size = channel.read(lengthBuffer, p);
            if(size == -1) {
                return null;
            }

            p += size;

            if(p == i) {
                ByteBuffer crc32CBuffer = BufferUtils.longByteBuffer();
                size = channel.read(crc32CBuffer, p);
                p += size;

                ByteBuffer buffer = ByteBuffer.allocate(length - LONG_LENGTH);
                channel.read(buffer, p);
                return buffer.array();
            }

            p += length;
        }

        return null;
    }

    @Override
    public String toString() {
        return channel.toString();
    }
}
