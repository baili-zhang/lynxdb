package com.bailizhang.lynxdb.raft.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.raft.common.RaftConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

/**
 * 最小 index 最大 index 索引区 数据区
 *
 * 维护内存数据和磁盘数据
 */
public class LogRegion implements AutoCloseable {
    private static final int DEFAULT_INDEX_SIZE = 200;

    private static final int BEGIN_POSITION = 0;
    private static final int END_POSITION = 4;
    private static final int INDEX_POSITION = 8;
    private static final int ENTRY_POSITION = 8 + DEFAULT_INDEX_SIZE * LogEntry.INDEX_BYTES_LENGTH;

    private static final int DEFAULT_NAME_LENGTH = 8;
    private static final String ZERO = "0";

    private static final String groupDir = RaftConfiguration.getInstance().logDir();

    private final File file;

    private final FileChannel channel;
    private final List<LogEntry> entries = new ArrayList<>();

    private int begin;
    private int end;

    public LogRegion(int id) {
        Path path = Path.of(groupDir, name(id));

        file = path.toFile();

        if(!file.exists()) {
            throw new RuntimeException(path.toString());
        }

        try {
            channel = FileChannel.open(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            if(channel.size() < ENTRY_POSITION) {
                initRegion();
            } else {
                readBegin();
                readEnd();
                readIndex();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static LogRegion create(int id, int begin) {
        Path path = Path.of(groupDir, name(id));

        File file = path.toFile();

        if(file.exists()) {
            String template = "Region \"%d\" file exists";
            throw new RuntimeException(String.format(template, id));
        }

        LogRegion region;
        try {
            region = new LogRegion(id);
            region.writeBegin(begin);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return region;
    }

    public int begin() {
        return begin;
    }

    public int end() {
        return end;
    }

    public void close() throws IOException {
        channel.close();
    }

    public void append(LogEntry entry) {
        byte[] data = entry.data();

        CRC32C crc32C = new CRC32C();
        crc32C.update(data);
        long crc32CValue = crc32C.getValue();

        ByteBuffer crc32CBuffer = BufferUtils.longByteBuffer(crc32CValue);
        ByteBuffer entryBuffer = ByteBuffer.wrap(data);
        ByteBuffer[] buffers = new ByteBuffer[]{crc32CBuffer, entryBuffer};

        try {
            channel.write(buffers);
            writeIndex(entry);
            writeEnd(++ end);
            channel.force(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public LogEntry readEntry(int idx) {
        if(idx < begin && idx > end) {
            return null;
        }

        int i = idx - begin;

        LogEntry entry = entries.get(i);
        int dataLen = idx == end ? (int)(length() - entry.dataBegin())
                : (int)(entries.get(i + 1).dataBegin() - entry.dataBegin());

        ByteBuffer crc32CBuffer = BufferUtils.longByteBuffer();
        ByteBuffer buffer = ByteBuffer.allocate(dataLen);

        try {
            channel.read(crc32CBuffer, entry.dataBegin());
            channel.read(buffer, entry.dataBegin() + LONG_LENGTH);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        long originCrc32C = crc32CBuffer.rewind().getLong();
        CRC32C crc32C = new CRC32C();
        crc32C.update(buffer.rewind());

        if(crc32C.getValue() != originCrc32C) {
            throw new RuntimeException("File entry data wrong.");
        }

        byte[] data = buffer.array();

        return new LogEntry(
                entry.method(),
                entry.invalid(),
                entry.type(),
                entry.dataBegin(),
                data
        );
    }

    public void delete() {
        if(!file.delete()) {
            throw new RuntimeException("Delete file failed");
        }
    }

    public long length() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return channel.toString();
    }

    private static String name(int id) {
        String idStr = String.valueOf(id);
        int zeroCount = DEFAULT_NAME_LENGTH - idStr.length();
        return ZERO.repeat(Math.max(0, zeroCount)) + idStr;
    }

    private void readBegin() throws IOException {
        ByteBuffer beginBuffer = BufferUtils.intByteBuffer();
        channel.read(beginBuffer, BEGIN_POSITION);
        begin = beginBuffer.rewind().getInt();
    }

    private void readEnd() throws IOException {
        ByteBuffer endBuffer = BufferUtils.intByteBuffer();
        channel.read(endBuffer, END_POSITION);
        end = endBuffer.rewind().getInt();
    }

    private void readIndex() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(LogEntry.INDEX_BYTES_LENGTH);
        long p = ENTRY_POSITION;

        while(true) {
            channel.read(buffer, p);

            if(buffer.get(0) == BufferUtils.EMPTY_BYTE) {
                break;
            }

            LogEntry entry = LogEntry.from(buffer);
            entries.add(entry);

            p += LogEntry.INDEX_BYTES_LENGTH;
        }
    }

    private void writeBegin(int begin) throws IOException {
        ByteBuffer beginBuffer = BufferUtils.intByteBuffer(begin);
        channel.write(beginBuffer, BEGIN_POSITION);
    }

    private void writeEnd(int end) throws IOException {
        ByteBuffer endBuffer = BufferUtils.intByteBuffer(end);
        channel.write(endBuffer, END_POSITION);
    }

    private void writeIndex(LogEntry entry) throws IOException {
        byte[] indexBytes = entry.toBytesList().toBytes();
        ByteBuffer buffer = ByteBuffer.wrap(indexBytes);
        long p = INDEX_POSITION + (long) entries.size() * LogEntry.INDEX_BYTES_LENGTH;
        channel.write(buffer, p);
    }

    private void initRegion() throws IOException {
        byte[] blank = new byte[ENTRY_POSITION];
        ByteBuffer buffer = ByteBuffer.wrap(blank);
        channel.write(buffer, BEGIN_POSITION);
    }
}
