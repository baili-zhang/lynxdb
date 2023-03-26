package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.mmap.MappedBuffer;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;

import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

/**
 * 维护内存数据和磁盘数据
 * 元数据区 索引区 数据区
 */
public class LogRegion {

    private interface Default {
        int DATA_BLOCK_SIZE = 1024;
        int CAPACITY = 2000;
    }

    private interface Meta {
        int LENGTH = INT_LENGTH * 2 + LONG_LENGTH;
        int BEGIN_POSITION = 0;
        int END_POSITION = INT_LENGTH;
        int CRC_POSITION = INT_LENGTH * 2;
    }

    private final int capacity;
    private final int indexBeginPosition = Meta.LENGTH;
    private final int dataBeginPosition;
    private final int indexEntryLength;
    private final int indexBlockLength;

    private final int id;
    private final Path path;
    private final LogGroupOptions options;

    private final MappedBuffer metaBuffer;
    private final MappedBuffer indexBuffer;
    private final ArrayList<MappedBuffer> dataBuffers = new ArrayList<>();

    public LogRegion(int id, String dir, LogGroupOptions options) {
        this.id = id;
        this.options = options;

        capacity = options.regionCapacityOrDefault(Default.CAPACITY);

        indexEntryLength = LogIndex.FIXED_LENGTH + options.extraDataLength();
        indexBlockLength = indexEntryLength * capacity;

        dataBeginPosition = indexBeginPosition + indexBlockLength;

        path = Path.of(dir, NameUtils.name(id));
        FileUtils.createFileIfNotExisted(path.toFile());

        metaBuffer = new MappedBuffer(
                path,
                Meta.BEGIN_POSITION,
                Meta.LENGTH
        );

        // 初始化 begin 和 end 的索引值为 -1
        if(globalIdxBegin() == 0) {
            globalIdxBegin(id * capacity);
            globalIdxEnd(id * capacity - 1);
        }

        indexBuffer = new MappedBuffer(
                path,
                indexBeginPosition,
                indexBlockLength
        );

        int dataBlockLength = dataBlockLength();
        int dataBlockCount = dataBlockLength / Default.DATA_BLOCK_SIZE + 1;

        for(int i = 0; i < dataBlockCount; i ++) {
            MappedBuffer dataBuffer = new MappedBuffer(
                    path,
                    dataBeginPosition + i * (long) Default.DATA_BLOCK_SIZE,
                    Default.DATA_BLOCK_SIZE);
            dataBuffers.add(dataBuffer);
        }
    }

    public int globalIdxBegin() {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        return buffer.getInt(Meta.BEGIN_POSITION);
    }

    public int globalIdxEnd() {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        return buffer.getInt(Meta.END_POSITION);
    }

    void globalIdxBegin(int val) {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        buffer.putInt(Meta.BEGIN_POSITION, val);

        buffer.position(0);
        buffer.limit(Meta.CRC_POSITION);

        CRC32C crc32C = new CRC32C();
        crc32C.update(buffer);
        long crc32c = crc32C.getValue();

        buffer.limit(Meta.LENGTH);
        buffer.putLong(Meta.CRC_POSITION, crc32c);

        if(options.isForce()) {
            buffer.force();
        }
    }

    void globalIdxEnd(int val) {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        buffer.putInt(Meta.END_POSITION, val);

        buffer.position(0);
        buffer.limit(Meta.CRC_POSITION);

        CRC32C crc32C = new CRC32C();
        crc32C.update(buffer);
        long crc32c = crc32C.getValue();

        buffer.limit(Meta.LENGTH);
        buffer.putLong(Meta.CRC_POSITION, crc32c);

        if(options.isForce()) {
            buffer.force();
        }
    }

    public int id() {
        return id;
    }

    public int append(byte[] extraData, byte[] data) {
        int globalIndexEnd = globalIdxEnd();
        LogIndex lastIndex = logIndex(globalIndexEnd);

        int idx = globalIndexEnd - globalIdxBegin() + 1;

        int dataBegin = lastIndex == null ? 0 : lastIndex.dataBegin() + lastIndex.dataLength();
        int dataLength = data.length + LONG_LENGTH; // data 长度 + crc32c 校验的长度

        CRC32C indexCrc32C = new CRC32C();
        indexCrc32C.update(extraData);
        indexCrc32C.update(dataBegin);
        indexCrc32C.update(dataLength);
        long indexCrc32c = indexCrc32C.getValue();

        LogIndex index = new LogIndex(
                options.extraDataLength(),
                extraData,
                dataBegin,
                dataLength,
                indexCrc32c
        );

        int indexOffset = idx * indexEntryLength;

        MappedByteBuffer indexByteBuffer = indexBuffer.getBuffer();
        indexByteBuffer.put(indexOffset, index.toBytes());

        if(options.isForce()) {
            indexByteBuffer.force();
        }

        MappedBuffer dataBuffer = dataBuffers.get(dataBuffers.size() - 1);
        if(dataBuffer == null) {
            dataBuffer = new MappedBuffer(
                    path,
                    (long) Default.DATA_BLOCK_SIZE * dataBuffers.size(),
                    Default.DATA_BLOCK_SIZE
            );
            dataBuffers.add(dataBuffer);
        }

        CRC32C dataCrc32C = new CRC32C();
        dataCrc32C.update(data);
        long dataCrc32c = dataCrc32C.getValue();

        LogDataEntry dataEntry = new LogDataEntry(data, dataCrc32c);

        MappedByteBuffer dataByteBuffer = dataBuffer.getBuffer();
        int remaining = dataByteBuffer.remaining();
        byte[] dateEntryBytes = dataEntry.toBytes();

        int dataBlockBegin = dataBegin - Default.DATA_BLOCK_SIZE * (dataBuffers.size() - 1);

        if(remaining < dataLength) {
            // 写入最后一个数据块的剩余空间
            dataByteBuffer.put(dataBlockBegin, dateEntryBytes, 0, remaining);
            dataBuffer.saveSnapshot(dataByteBuffer);

            if(options.isForce()) {
                dataBuffer.force();
            }

            // TODO: 处理超大数据块

            // 写入一个新的数据块
            dataBuffer = new MappedBuffer(
                    path,
                    (long) Default.DATA_BLOCK_SIZE * dataBuffers.size(),
                    Default.DATA_BLOCK_SIZE
            );

            dataBuffers.add(dataBuffer);
            dataByteBuffer = dataBuffer.getBuffer();
            dataByteBuffer.put(0, dateEntryBytes, remaining, dataLength - remaining);
        } else {
            dataByteBuffer.put(dataBlockBegin, dateEntryBytes, 0, dataLength);
        }

        dataBuffer.saveSnapshot(dataByteBuffer);

        if(options.isForce()) {
            dataByteBuffer.force();
        }

        globalIdxEnd(++ globalIndexEnd);
        return globalIndexEnd;
    }

    public LogIndex logIndex(int globalIdx) {
        int idx = globalIdx - globalIdxBegin();
        if(idx < 0) {
            return null;
        }

        int indexBegin = idx * indexEntryLength;
        MappedByteBuffer buffer = indexBuffer.getBuffer();
        buffer.position(indexBegin);

        return LogIndex.from(buffer, options.extraDataLength());
    }

    public LogEntry readEntry(int globalIndex) {
        if(globalIndex < globalIdxBegin() || globalIndex > globalIdxEnd()) {
            return null;
        }

        LogIndex logIndex = logIndex(globalIndex);
        int dataBegin = logIndex.dataBegin();
        int dataLength = logIndex.dataLength();
        int rawDataLength = dataLength - LONG_LENGTH;   // 减去 crc32c 校验位的长度

        byte[] data = new byte[rawDataLength];
        int bufferIdx = dataBegin / Default.DATA_BLOCK_SIZE;

        if(bufferIdx >= dataBuffers.size()) {
            throw new RuntimeException();
        }

        MappedBuffer dataBuffer = dataBuffers.get(bufferIdx);
        MappedByteBuffer buffer = dataBuffer.getBuffer();

        buffer.position(dataBegin - bufferIdx * Default.DATA_BLOCK_SIZE);
        int remaining = buffer.remaining();

        if(remaining < rawDataLength) {
            buffer.get(data, 0, remaining);
            dataBuffer = dataBuffers.get(bufferIdx + 1);
            buffer = dataBuffer.getBuffer();
            buffer.get(0, data, remaining, rawDataLength - remaining);
        } else {
            buffer.get(data, 0, rawDataLength);
        }

        remaining = buffer.remaining();
        byte[] crc32cBytes = new byte[LONG_LENGTH];

        if(remaining < LONG_LENGTH) {
            buffer.get(crc32cBytes, 0, remaining);
            dataBuffer = dataBuffers.get(bufferIdx + 1);
            buffer = dataBuffer.getBuffer();
            buffer.get(0, crc32cBytes, remaining, LONG_LENGTH - remaining);
        } else {
            buffer.get(crc32cBytes, 0, LONG_LENGTH);
        }

        long originCrc32C = ByteArrayUtils.toLong(crc32cBytes);

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

    public byte[] lastExtraData() {
        LogIndex index = logIndex(globalIdxEnd());
        return index == null ? null : index.extraData();
    }

    public void delete() {
        FileUtils.delete(path);
    }

    public int dataBlockLength() {
        LogIndex lastLogIndex = logIndex(globalIdxEnd());
        if(lastLogIndex == null) {
            return 0;
        }
        return lastLogIndex.dataBegin() + lastLogIndex.dataLength();
    }

    public boolean isFull() {
        return globalIdxEnd() - globalIdxBegin() + 1 >= capacity;
    }
}
