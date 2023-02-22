package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.mmap.MappedBuffer;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;

import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

/**
 * 最小 index 最大 index 索引区 数据区
 *
 * 维护内存数据和磁盘数据
 */
public class LogRegion {
    private static final int DEFAULT_LOG_REGION_SIZE = 2000;

    private static final int BEGIN_FIELD_POSITION = 0;
    private static final int END_FIELD_POSITION = 4;
    private static final int INDEX_BEGIN_POSITION = 8;

    private final int dataBeginPosition;
    private final int logIndexLength;
    
    private final int logRegionSize;

    private final int id;
    private final Path path;
    private final LogGroupOptions options;

    private final MappedBuffer mappedBuffer;

    public LogRegion(int id, String dir, LogGroupOptions options) {
        this.id = id;

        this.options = options;

        Integer logRegionSize = options.logRegionSize();
        this.logRegionSize = logRegionSize == null ? DEFAULT_LOG_REGION_SIZE : logRegionSize;

        logIndexLength = LogIndex.FIXED_LENGTH + options.extraDataLength();
        dataBeginPosition = INDEX_BEGIN_POSITION + this.logRegionSize * logIndexLength;

        path = Path.of(dir, NameUtils.name(id));
        FileUtils.createFileIfNotExisted(path.toFile());

        mappedBuffer = new MappedBuffer(
                path,
                BEGIN_FIELD_POSITION,
                LogGroup.DEFAULT_FILE_THRESHOLD
        );
    }

    public int globalIndexBegin() {
        return mappedBuffer().getInt(BEGIN_FIELD_POSITION);
    }

    public int globalIndexEnd() {
        return mappedBuffer().getInt(END_FIELD_POSITION);
    }

    public void globalIndexBegin(int val) {
        mappedBuffer().putInt(BEGIN_FIELD_POSITION, val);
    }

    public void globalIndexEnd(int val) {
        mappedBuffer().putInt(END_FIELD_POSITION, val);
    }

    public int id() {
        return id;
    }

    public int append(byte[] extraData, byte[] data) {
        int globalIndexEnd = globalIndexEnd();
        LogIndex lastIndex = logIndex(globalIndexEnd);

        int dataBegin = lastIndex == null
                ? dataBeginPosition
                : (lastIndex.dataBegin() + lastIndex.dataLength() + LONG_LENGTH);

        int dataLength = data.length;

        LogIndex index = new LogIndex(
                options.extraDataLength(),
                extraData,
                dataBegin,
                dataLength
        );

        int indexBegin = INDEX_BEGIN_POSITION +
                (globalIndexEnd() - globalIndexBegin() + 1) * logIndexLength;
        mappedBuffer().position(indexBegin);
        index.toBytesList().forEach(mappedBuffer()::put);

        CRC32C crc32C = new CRC32C();
        crc32C.update(data);
        long crc32CValue = crc32C.getValue();
        int crc32CValueBegin = dataBegin + dataLength;

        mappedBuffer().put(dataBegin, data);
        mappedBuffer().putLong(crc32CValueBegin, crc32CValue);
        globalIndexEnd(++ globalIndexEnd);

        if(options.forceAfterEachAppend()) {
            force();
        }

        return globalIndexEnd;
    }

    public LogIndex logIndex(int globalIndex) {
        int count = globalIndex - globalIndexBegin();
        if(count < 0) {
            return null;
        }

        int lastIndexBegin = INDEX_BEGIN_POSITION + count * logIndexLength;

        mappedBuffer().position(lastIndexBegin);
        return LogIndex.from(mappedBuffer(), options.extraDataLength());
    }

    public LogEntry readEntry(int globalIndex) {
        if(globalIndex < globalIndexBegin() || globalIndex > globalIndexEnd()) {
            return null;
        }

        LogIndex logIndex = logIndex(globalIndex);
        int dataBegin = logIndex.dataBegin();
        int dataLength = logIndex.dataLength();

        byte[] data = new byte[dataLength];

        mappedBuffer().position(dataBegin);
        mappedBuffer().get(data, 0, dataLength);
        long originCrc32C = mappedBuffer().getLong();

        CRC32C crc32C = new CRC32C();
        crc32C.update(data);

        if(crc32C.getValue() != originCrc32C) {
            throw new RuntimeException("File entry data wrong.");
        }

        return new LogEntry(
                logIndex,
                data
        );
    }

    public void force() {
        mappedBuffer().force();
    }

    public void delete() {
        FileUtils.delete(path);
    }

    public long length() {
        LogIndex lastLogIndex = logIndex(globalIndexEnd());
        if(lastLogIndex == null) {
            return 0;
        }
        return lastLogIndex.dataBegin() + lastLogIndex.dataLength();
    }

    public boolean isFull() {
        return globalIndexEnd() - globalIndexBegin() + 1 >= logRegionSize;
    }

    private MappedByteBuffer mappedBuffer() {
        return mappedBuffer.getBuffer();
    }
}
