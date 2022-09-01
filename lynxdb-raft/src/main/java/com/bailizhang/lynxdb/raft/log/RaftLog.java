package com.bailizhang.lynxdb.raft.log;

import com.bailizhang.lynxdb.raft.state.RaftState;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.enhance.EnhanceFile;

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Raft Log
 *
 * 1. 索引文件（indexFile）
 * 2. 数据文件（dataFile）
 *
 * 索引文件：索引的最小值从 1 开始，第 0 位索引的 12 个 bytes 前四位用于存储最大有效的索引位
 *         后 8 位 bytes 暂时保留不用。
 * 索引文件格式：
 * | term    | offset  | length  |
 * | ------  | ------- | ------- |
 * | 4 bytes | 4 bytes | 4 bytes |
 *
 * 第 0 位的索引：
 * | max index value | (reserved bits) |
 * | --------------- | --------------- |
 * | 4 bytes         | 12 bytes        |
 */
public class RaftLog {
    private static final int MAX_INDEX_VALUE_POSITION = 0;

    private static final String DEFAULT_RAFT_LOG_DIR = System.getProperty("user.dir") + "/logs";
    private static final String DEFAULT_RAFT_INDEX_LOG_FILENAME = "raft_log.index";
    private static final String DEFAULT_RAFT_DATA_LOG_FILENAME = "raft_log.data";

    private final EnhanceFile indexFile;
    private final EnhanceFile dataFile;

    /**
     * 当 index == 0 时，返回 BEGIN_ENTRY
     */
    public static final RaftLogEntry BEGIN_RAFT_LOG_ENTRY = new RaftLogEntry(
            null, 0, 0, RaftState.DATA_CHANGE, new byte[0]);

    public RaftLog() throws IOException {
        this(DEFAULT_RAFT_INDEX_LOG_FILENAME,
                DEFAULT_RAFT_DATA_LOG_FILENAME);
    }

    public RaftLog(String indexFilename, String dataFilename) throws IOException {
        indexFile = new EnhanceFile(DEFAULT_RAFT_LOG_DIR, indexFilename);
        dataFile = new EnhanceFile(DEFAULT_RAFT_LOG_DIR, dataFilename);

        /* 如果是新文件，则设置最大索引值，避免最大索引值读不到的问题 */
        if(indexFile.length() == 0) {
            setMaxIndexValue(0);
        }
    }

    private void setMaxIndexValue(int value) throws IOException {
        ByteBuffer indexBuffer = BufferUtils.intByteBuffer();
        indexBuffer.putInt(value).rewind();
        indexFile.write(indexBuffer, MAX_INDEX_VALUE_POSITION);
    }

    public synchronized void setMaxIndex(int value) throws IOException {
        setMaxIndexValue(value);
    }

    private int getMaxIndexValue() throws IOException {
        ByteBuffer indexBuffer = BufferUtils.intByteBuffer();
        indexFile.read(indexBuffer, MAX_INDEX_VALUE_POSITION);
        return indexBuffer.rewind().getInt();
    }

    /**
     * 同步方法，写入索引和写入数据应该是原子操作
     * @param raftLogEntry 尾部添加的的日志条目
     */
    public synchronized int append(RaftLogEntry raftLogEntry) {
        try {
            /* 找上一个日志 entry，并得到数据文件写入的 offset */
            int maxIndexValue = getMaxIndexValue(), dataOffset = 0;
            if(maxIndexValue != 0) {
                EntryIndex entryIndex = getEntryIndexByIndex(maxIndexValue);
                dataOffset = entryIndex.offset() + entryIndex.length();
            }

            /* 将 command 写入数据文件 */
            byte[] command = raftLogEntry.command();
            int length = command.length;
            dataFile.write(ByteBuffer.wrap(command), dataOffset);

            /* 将 entry 的索引数据写入索引文件 */
            EntryIndex index = new EntryIndex(raftLogEntry.term(), dataOffset, length);
            indexFile.write(ByteBuffer.wrap(index.toBytes()),
                    ((long) (++ maxIndexValue)) * EntryIndex.ENTRY_INDEX_LENGTH);

            setMaxIndexValue(maxIndexValue);
            return maxIndexValue;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(RaftLogEntry[] entries) throws IOException {
        for(RaftLogEntry raftLogEntry : entries) {
            append(raftLogEntry);
        }
    }

    public RaftLogEntry lastEntry() throws IOException {
        return getEntryByIndex(getMaxIndexValue());
    }

    public int indexOfLastLogEntry() throws IOException {
        return getMaxIndexValue();
    }

    public synchronized RaftLogEntry getEntryByIndex(int index) throws IOException {
        if(index == 0) {
            return BEGIN_RAFT_LOG_ENTRY;
        }

        if(index > getMaxIndexValue()) {
            return null;
        }

        EntryIndex entryIndex = getEntryIndexByIndex(index);
        ByteBuffer command = ByteBuffer.allocate(entryIndex.length());
        dataFile.read(command, entryIndex.offset());
        return new RaftLogEntry(null, 0, entryIndex.term(), RaftState.DATA_CHANGE, command.array());
    }

    private synchronized EntryIndex getEntryIndexByIndex(int index) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(EntryIndex.ENTRY_INDEX_LENGTH);
        indexFile.read(buffer, ((long) index) * (long) EntryIndex.ENTRY_INDEX_LENGTH);
        return EntryIndex.fromBytes(buffer.array());
    }

    public int getEntryTermByIndex(int index) throws IOException {
        EntryIndex entryIndex = getEntryIndexByIndex(index);
        return entryIndex.term();
    }

    /**
     * 获取 commitIndex 在前开后闭区间 (begin, end] 上的所有日志条目
     * @param begin 开始位置
     * @param end 结束位置
     * @return 区间中的所有日志条目
     * @throws IOException IO异常
     */
    public synchronized RaftLogEntry[] getEntriesByRange(int begin, int end) throws IOException {
        assert end > begin && begin > 0;
        RaftLogEntry[] entries = new RaftLogEntry[end - begin];

        for (int i = 0; i < entries.length; i++) {
            entries[i] = getEntryByIndex(begin + i + 1);
        }

        return entries;
    }

    public void delete() throws IOException {
        indexFile.delete();
        dataFile.delete();
    }
}
