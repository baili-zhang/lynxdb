package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.FileType;
import com.bailizhang.lynxdb.core.common.Flags;
import com.bailizhang.lynxdb.core.common.Pair;
import com.bailizhang.lynxdb.core.mmap.MappedBuffer;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;

import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

/**
 * 维护内存数据和磁盘数据
 * 元数据区 索引区 数据区
 */
public class LogRegion {
    private interface Default {
        int DATA_BLOCK_SIZE = 1024 * 1024;
        int CAPACITY = 2000;
        double CLEAR_THRESHOLD = 0.5d;
    }

    private interface Meta {
        int LENGTH = INT_LENGTH * 5 + LONG_LENGTH;
        int MAGIC_NUMBER_POSITION = 0;
        int DELETED_LENGTH_POSITION = INT_LENGTH;
        int TOTAL_LENGTH_POSITION = INT_LENGTH * 2;
        int BEGIN_GLOBAL_ID_POSITION = INT_LENGTH * 3;
        int END_GLOBAL_ID_POSITION = INT_LENGTH * 4;
        int CRC_POSITION = INT_LENGTH * 5;
    }

    private final int capacity;
    private final int dataBeginPosition;

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
        // index 区域的总长度
        int indexBlockLength = LogIndex.ENTRY_LENGTH * capacity;
        // data 区域的开始位置
        dataBeginPosition = Meta.LENGTH + indexBlockLength;

        path = Path.of(dir, NameUtils.name(id) + FileType.LOG_GROUP_REGION_FILE.suffix());
        FileUtils.createFileIfNotExisted(path.toFile());

        metaBuffer = new MappedBuffer(
                path,
                Meta.MAGIC_NUMBER_POSITION,
                Meta.LENGTH
        );

        // 初始化 meta 区域
        if(magicNumber() == 0) {
            MappedByteBuffer buffer = metaBuffer.getBuffer();
            buffer.putInt(Meta.MAGIC_NUMBER_POSITION, FileType.LOG_GROUP_REGION_FILE.magicNumber());
            buffer.putInt(Meta.DELETED_LENGTH_POSITION, 0);
            buffer.putInt(Meta.TOTAL_LENGTH_POSITION, 0);
            buffer.putInt(Meta.BEGIN_GLOBAL_ID_POSITION, (id - 1) * capacity + 1);
            buffer.putInt(Meta.END_GLOBAL_ID_POSITION, (id - 1) * capacity);
            generateMetaCrc();
        }

        indexBuffer = new MappedBuffer(
                path,
                // index 区域的开始位置
                Meta.LENGTH,
                indexBlockLength
        );

        int dataBlockLength = dataBlockLength();
        int dataBlockCount = dataBlockLength / Default.DATA_BLOCK_SIZE + 1;

        for(int i = 0; i < dataBlockCount; i ++) {
            MappedBuffer dataBuffer = mapDataBlockBuffer(i);
            dataBuffers.add(dataBuffer);
        }
    }

    public int magicNumber() {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        return buffer.getInt(Meta.MAGIC_NUMBER_POSITION);
    }

    public int deletedLength() {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        return buffer.getInt(Meta.DELETED_LENGTH_POSITION);
    }

    public int totalLength() {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        return buffer.getInt(Meta.TOTAL_LENGTH_POSITION);
    }

    public int globalIdxBegin() {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        return buffer.getInt(Meta.BEGIN_GLOBAL_ID_POSITION);
    }

    public int globalIdxEnd() {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        return buffer.getInt(Meta.END_GLOBAL_ID_POSITION);
    }

    public void deletedLength(int len) {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        int newLen = buffer.getInt(Meta.DELETED_LENGTH_POSITION) + len;
        buffer.putInt(Meta.DELETED_LENGTH_POSITION, newLen);

        generateMetaCrc();
    }

    public void totalLength(int len) {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        int newLen = buffer.getInt(Meta.TOTAL_LENGTH_POSITION) + len;
        buffer.putInt(Meta.TOTAL_LENGTH_POSITION, newLen);

        generateMetaCrc();
    }

    void globalIdxBegin(int val) {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        buffer.putInt(Meta.BEGIN_GLOBAL_ID_POSITION, val);

        generateMetaCrc();
    }

    void globalIdxEnd(int val) {
        MappedByteBuffer buffer = metaBuffer.getBuffer();
        buffer.putInt(Meta.END_GLOBAL_ID_POSITION, val);

        generateMetaCrc();
    }

    public int id() {
        return id;
    }

    public int appendEntry(byte[] data) {
        return appendEntry(Flags.EXISTED, data);
    }

    public int appendEntry(byte deleteFlag, byte[] data) {
        int globalIndexEnd = globalIdxEnd();
        LogIndex lastIndex = logIndex(globalIndexEnd);

        int idx = globalIndexEnd - globalIdxBegin() + 1;

        int dataBegin = lastIndex == null ? 0 : lastIndex.dataBegin() + lastIndex.dataLength();
        int dataLength = data.length + LONG_LENGTH; // data 长度 + crc32c 校验的长度

        LogIndex index = LogIndex.from(
                deleteFlag,
                dataBegin,
                dataLength
        );

        int indexOffset = idx * LogIndex.ENTRY_LENGTH;

        MappedByteBuffer indexByteBuffer = indexBuffer.getBuffer();
        indexByteBuffer.put(indexOffset, index.toBytes());

        if(options.isForce()) {
            indexByteBuffer.force();
        }

        DataEntry dataEntry = DataEntry.from(data);

        // data 块中的起始位置，也就是当前数据写入的位置
        int dataBlockBegin = dataBegin - Default.DATA_BLOCK_SIZE * (dataBuffers.size() - 1);
        BytesList dataByteList = dataEntry.toBytesList();

        writeData(dataByteList, dataBlockBegin);

        // 更新总长度（包括 CRC 校验的长度）
        totalLength(dataLength);

        globalIdxEnd(++ globalIndexEnd);
        return globalIndexEnd;
    }

    public List<Pair<Byte, byte[]>> aliveEntries() {
        double deletedLength = deletedLength();
        double totalLength = totalLength();

        double percentage = deletedLength / totalLength;

        if(percentage < Default.CLEAR_THRESHOLD) {
            return null;
        }

        // 如果达到清理的阈值，则执行清理操作
        int globalIdxBegin = globalIdxBegin();
        int globalIdxEnd = globalIdxEnd();

        List<Pair<Byte, byte[]>> entries = new ArrayList<>();

        for(int i = globalIdxBegin; i <= globalIdxEnd; i ++) {
            LogEntry entry = readEntry(i);
            LogIndex index = entry.index();

            byte deleteFlag = index.deleteFlag();
            byte[] data = deleteFlag == Flags.DELETED ? ByteArrayUtils.EMPTY_BYTES : entry.data();

            entries.add(new Pair<>(deleteFlag, data));
        }

        return entries;
    }

    public LogIndex logIndex(int globalIdx) {
        int idx = globalIdx - globalIdxBegin();
        if(idx < 0) {
            return null;
        }

        int indexBegin = idx * LogIndex.ENTRY_LENGTH;
        MappedByteBuffer buffer = indexBuffer.getBuffer();
        buffer.position(indexBegin);

        return LogIndex.from(buffer);
    }

    public LogEntry readEntry(int globalIndex) {
        if(globalIndex < globalIdxBegin() || globalIndex > globalIdxEnd()) {
            return null;
        }

        LogIndex logIndex = logIndex(globalIndex);
        int dataBegin = logIndex.dataBegin();
        int dataLength = logIndex.dataLength();
        // 纯数据的长度，不包括 CRC 校验和
        int rawDataLength = dataLength - LONG_LENGTH;

        byte[] data = readData(dataBegin, rawDataLength);
        byte[] crc32cBytes = readData(dataBegin + rawDataLength, LONG_LENGTH);

        CRC32C crc32C = new CRC32C();
        crc32C.update(data, 0, dataLength - LONG_LENGTH);

        long originCrc32C = ByteArrayUtils.toLong(crc32cBytes);

        if(crc32C.getValue() != originCrc32C) {
            throw new RuntimeException("File entry data wrong.");
        }

        return new LogEntry(
                logIndex,
                data
        );
    }

    public void removeEntry(int globalIdx) {
        LogIndex logIndex = logIndex(globalIdx);

        int dataBegin = logIndex.dataBegin();
        // 包括 CRC 校验和的长度
        int dataLength = logIndex.dataLength();

        LogIndex newLogIndex = LogIndex.from(Flags.DELETED, dataBegin, dataLength);
        MappedByteBuffer buffer = indexBuffer.getBuffer();

        int indexOffset = (globalIdx - globalIdxBegin()) * LogIndex.ENTRY_LENGTH;
        buffer.put(indexOffset, newLogIndex.toBytes());

        // 删除的长度包括数据的 CRC 校验和
        deletedLength(dataLength);
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

    private void generateMetaCrc() {
        MappedByteBuffer buffer = metaBuffer.getBuffer();

        buffer.position(0);

        // CRC 校验内容只包括 Meta 的数据，不包括校验和
        buffer.limit(Meta.CRC_POSITION);

        CRC32C crc32C = new CRC32C();
        crc32C.update(buffer);
        long crc32c = crc32C.getValue();

        // 重置 limit，用于写入校验和
        buffer.limit(Meta.LENGTH);
        buffer.putLong(Meta.CRC_POSITION, crc32c);

        if(options.isForce()) {
            buffer.force();
        }
    }

    private MappedBuffer mapDataBlockBuffer(int i) {
        return new MappedBuffer(
                path,
                dataBeginPosition + (long) Default.DATA_BLOCK_SIZE * i,
                Default.DATA_BLOCK_SIZE
        );
    }

    private void writeData(BytesList dataByteList, int begin) {
        // 拿到最后一个 dataBuffer
        MappedBuffer dataBuffer = dataBuffers.get(dataBuffers.size() - 1);
        if(dataBuffer == null) {
            dataBuffer = mapDataBlockBuffer(dataBuffers.size());
            dataBuffers.add(dataBuffer);
        }

        MappedByteBuffer dataByteBuffer = dataBuffer.getBuffer();
        dataByteBuffer.position(begin);

        while(BufferUtils.isNotOver(dataByteBuffer)) {
            // 写入最后一个数据块的剩余空间
            dataByteList.writeIntoBuffer(dataByteBuffer);
            dataBuffer.saveSnapshot(dataByteBuffer);

            if(options.isForce()) {
                dataBuffer.force();
            }

            // 如果 dataByteBuffer 全部写满了，创建新的 buffer
            if(BufferUtils.isOver(dataByteBuffer)) {
                dataBuffer = mapDataBlockBuffer(dataBuffers.size());
                dataBuffers.add(dataBuffer);
                dataByteBuffer = dataBuffer.getBuffer();
            }
        }

        if(options.isForce()) {
            dataByteBuffer.force();
        }
    }

    private byte[] readData(int begin, int length) {
        int bufferIdx = begin / Default.DATA_BLOCK_SIZE;
        byte[] data = new byte[length];

        if(bufferIdx >= dataBuffers.size()) {
            throw new RuntimeException();
        }

        MappedBuffer dataBuffer = dataBuffers.get(bufferIdx);
        MappedByteBuffer buffer = dataBuffer.getBuffer();

        buffer.position(begin - bufferIdx * Default.DATA_BLOCK_SIZE);
        int dataOffset = 0;

        while (dataOffset < length) {
            int readLength = Math.min(buffer.remaining(), length - dataOffset);
            buffer.get(data, dataOffset, readLength);
            dataOffset += readLength;

            if(dataOffset < length) {
                dataBuffer = dataBuffers.get(++ bufferIdx);
                buffer = dataBuffer.getBuffer();
                buffer.position(0);
            }
        }

        return data;
    }
}
