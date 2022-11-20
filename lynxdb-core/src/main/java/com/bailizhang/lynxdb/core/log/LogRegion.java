package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

/**
 * 最小 index 最大 index 索引区 数据区
 *
 * 维护内存数据和磁盘数据
 */
public class LogRegion implements AutoCloseable {
    private static final int DEFAULT_INDEX_SIZE = 200;

    private static final int BEGIN_FIELD_POSITION = 0;
    private static final int END_FIELD_POSITION = 4;
    private static final int INDEX_BEGIN_POSITION = 8;

    private final int dataBeginPosition;
    private final int logIndexLength;

    private final int id;
    private final String groupDir;
    private final LogOptions options;

    private final File file;

    private final FileChannel channel;
    private final ArrayList<LogIndex> logIndexList = new ArrayList<>(DEFAULT_INDEX_SIZE);

    private int globalIndexBegin;
    private int globalIndexEnd;

    public LogRegion(int id, String dir, LogOptions options) {
        this.id = id;
        logIndexLength = LogIndex.FIXED_LENGTH + options.extraDataLength();
        dataBeginPosition = INDEX_BEGIN_POSITION + DEFAULT_INDEX_SIZE * logIndexLength;

        groupDir = dir;
        this.options = options;

        Path path = Path.of(groupDir, NameUtils.name(id));

        file = path.toFile();

        if(!file.exists()) {
            throw new RuntimeException(path.toString());
        }

        try {
            channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            if(channel.size() < dataBeginPosition) {
                initRegion();
            } else {
                readBegin();
                readEnd();
                loadLogIndex();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static LogRegion create(int id, int begin, String groupDir, LogOptions options) {
        FileUtils.createDirIfNotExisted(groupDir);

        Path path = Path.of(groupDir, NameUtils.name(id));
        File file = path.toFile();

        if(file.exists()) {
            String template = "Region \"%d\" file exists";
            throw new RuntimeException(String.format(template, id));
        }

        FileUtils.createFile(file);

        LogRegion region;
        try {
            region = new LogRegion(id, groupDir, options);
            region.writeBegin(begin);
            region.writeEnd(begin - 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return region;
    }

    public static LogRegion open(int id, String groupDir, LogOptions options) {
        Path path = Path.of(groupDir, NameUtils.name(id));

        File file = path.toFile();

        if(!file.exists()) {
            String template = "Region \"%d\" file not exists";
            throw new RuntimeException(String.format(template, id));
        }

        return new LogRegion(id, groupDir, options);
    }

    public int globalIndexBegin() {
        return globalIndexBegin;
    }

    public int globalIndexEnd() {
        return globalIndexEnd;
    }

    public int id() {
        return id;
    }

    public String groupDir() {
        return groupDir;
    }

    public void close() throws IOException {
        channel.close();
    }

    public int append(byte[] extraData, byte[] data) {
        long dataBegin;

        if(logIndexList.isEmpty()) {
            dataBegin = dataBeginPosition;
        } else {
            LogIndex lastIndex = logIndexList.get(logIndexList.size() - 1);
            dataBegin = lastIndex.dataBegin() + lastIndex.dataLength() + LONG_LENGTH;
        }

        int dataLength = data.length;
        LogIndex index = new LogIndex(
                options.extraDataLength(),
                extraData,
                dataBegin,
                dataLength
        );

        CRC32C crc32C = new CRC32C();
        crc32C.update(data);
        long crc32CValue = crc32C.getValue();
        long crc32CValueBegin = dataBegin + dataLength;

        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
        ByteBuffer crc32CBuffer = BufferUtils.longByteBuffer(crc32CValue);

        try {
            channel.write(dataBuffer, dataBegin);
            channel.write(crc32CBuffer, crc32CValueBegin);
            appendIndex(index);
            writeEnd(++globalIndexEnd);
            channel.force(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return globalIndexEnd;
    }

    public LogEntry readEntry(int globalIndex) {
        if(globalIndex < globalIndexBegin || globalIndex > globalIndexEnd) {
            return null;
        }

        int i = globalIndex - globalIndexBegin;

        LogIndex logIndex = logIndexList.get(i);
        long dataBegin = logIndex.dataBegin();
        int dataLength = logIndex.dataLength();

        ByteBuffer buffer = ByteBuffer.allocate(dataLength);
        ByteBuffer crc32CBuffer = BufferUtils.longByteBuffer();

        try {
            channel.read(buffer, dataBegin);
            channel.read(crc32CBuffer, dataBegin + dataLength);
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
                logIndex,
                data
        );
    }

    public byte[] extraData() {
        LogIndex logIndex = logIndexList.get(logIndexList.size() - 1);
        return logIndex.extraData();
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

    public boolean isFull() {
        return logIndexList.size() >= DEFAULT_INDEX_SIZE;
    }

    @Override
    public String toString() {
        return channel.toString();
    }

    private void readBegin() throws IOException {
        ByteBuffer beginBuffer = BufferUtils.intByteBuffer();
        channel.read(beginBuffer, BEGIN_FIELD_POSITION);
        globalIndexBegin = beginBuffer.rewind().getInt();
    }

    private void readEnd() throws IOException {
        ByteBuffer endBuffer = BufferUtils.intByteBuffer();
        channel.read(endBuffer, END_FIELD_POSITION);
        globalIndexEnd = endBuffer.rewind().getInt();
    }

    private void loadLogIndex() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(logIndexLength);
        long position = INDEX_BEGIN_POSITION;

        for(int size = globalIndexEnd - globalIndexBegin; size >= 0; size --) {
            channel.read(buffer, position);

            buffer.rewind();
            LogIndex index = LogIndex.from(buffer, options.extraDataLength());
            logIndexList.add(index);

            position += logIndexLength;
        }
    }

    private void writeBegin(int val) throws IOException {
        globalIndexBegin = val;
        ByteBuffer beginBuffer = BufferUtils.intByteBuffer(globalIndexBegin);
        channel.write(beginBuffer, BEGIN_FIELD_POSITION);
    }

    private void writeEnd(int val) throws IOException {
        globalIndexEnd = val;
        ByteBuffer endBuffer = BufferUtils.intByteBuffer(globalIndexEnd);
        channel.write(endBuffer, END_FIELD_POSITION);
    }

    private void appendIndex(LogIndex index) throws IOException {
        byte[] indexBytes = index.toBytes();
        ByteBuffer buffer = ByteBuffer.wrap(indexBytes);
        long position = INDEX_BEGIN_POSITION + (long) logIndexList.size() * logIndexLength;
        channel.write(buffer, position);
        logIndexList.add(index);
    }

    private void initRegion() throws IOException {
        byte[] blank = new byte[dataBeginPosition];
        ByteBuffer buffer = ByteBuffer.wrap(blank);
        channel.write(buffer, BEGIN_FIELD_POSITION);
    }
}
