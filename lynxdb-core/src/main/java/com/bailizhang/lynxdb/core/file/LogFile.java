package com.bailizhang.lynxdb.core.file;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

public class LogFile implements AutoCloseable {
    private static final int BEGIN_POSITION = 0;
    private static final int END_POSITION = 4;
    private static final int ENTRY_BEGIN_POSITION = 8;

    private final File file;

    private FileChannel channel;
    private int end;

    public LogFile(String dirname, String filename) throws IOException {
        Path path = Path.of(dirname, filename);

        file = path.toFile();

        if(!file.exists()) {
            return;
        }

        channel = FileChannel.open(path);
    }

    public void createFile(int begin) throws IOException {
        if(file.exists()) {
            return;
        }

        if(!file.createNewFile()) {
            throw new RuntimeException("Create file failed.");
        }

        channel = FileChannel.open(file.toPath());
        writeBegin(begin);
        writeEnd(begin);
    }

    public void writeBegin(int begin) throws IOException {
        ByteBuffer beginBuffer = BufferUtils.intByteBuffer(begin);
        channel.write(beginBuffer, BEGIN_POSITION);
        channel.force(false);
    }

    public void writeEnd(int end) throws IOException {
        ByteBuffer endBuffer = BufferUtils.intByteBuffer(end);
        channel.write(endBuffer, END_POSITION);
        channel.force(false);
    }

    public int begin() throws IOException {
        ByteBuffer beginBuffer = BufferUtils.intByteBuffer();
        channel.read(beginBuffer, BEGIN_POSITION);
        return beginBuffer.rewind().getInt();
    }

    public int end() throws IOException {
        ByteBuffer endBuffer = BufferUtils.intByteBuffer();
        channel.read(endBuffer, END_POSITION);
        return endBuffer.rewind().getInt();
    }

    public void close() throws IOException {
        channel.close();
    }

    public int append(BytesListConvertible convertible) throws IOException {
        writeEnd(++ end);

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

        return end;
    }

    public byte[] readEntry(int i) throws IOException {
        if(i < begin() && i > end()) {
            return null;
        }

        int p = ENTRY_BEGIN_POSITION;
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

                long originCrc32C = crc32CBuffer.rewind().getLong();
                CRC32C crc32C = new CRC32C();
                crc32C.update(buffer.rewind());

                if(crc32C.getValue() != originCrc32C) {
                    throw new RuntimeException("File entry data wrong.");
                }

                return buffer.array();
            }

            p += length;
        }

        return null;
    }

    public void delete() {
        if(!file.delete()) {
            throw new RuntimeException("Delete file failed");
        }
    }

    public long length() throws IOException {
        return channel.size();
    }

    @Override
    public String toString() {
        return channel.toString();
    }
}
