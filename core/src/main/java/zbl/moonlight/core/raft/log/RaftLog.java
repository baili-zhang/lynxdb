package zbl.moonlight.core.raft.log;

import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.utils.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftLog {
    private static final int ENTRY_INDEX_LENGTH = 16;

    public static final String DEFAULT_RAFT_LOG_DIR = System.getProperty("user.dir") + "/logs";
    public static final String DEFAULT_RAFT_INDEX_LOG_FILENAME = "raft_log.index";
    public static final String DEFAULT_RAFT_DATA_LOG_FILENAME = "raft_log.data";
    public static final String DEFAULT_RAFT_VERIFY_LOG_FILENAME = "raft_log.verify";

    private final EnhanceFile indexFile;
    private final EnhanceFile dataFile;
    private final EnhanceFile verifyFile;

    private final AtomicInteger cursor = new AtomicInteger(0);
    private final AtomicInteger position = new AtomicInteger(0);

    public RaftLog() throws IOException {
        this(DEFAULT_RAFT_INDEX_LOG_FILENAME,
                DEFAULT_RAFT_DATA_LOG_FILENAME,
                DEFAULT_RAFT_VERIFY_LOG_FILENAME);
    }

    public RaftLog(String indexFilename, String dataFilename, String verifyFilename) throws IOException {
        indexFile = new EnhanceFile(DEFAULT_RAFT_LOG_DIR, indexFilename);
        dataFile = new EnhanceFile(DEFAULT_RAFT_LOG_DIR, dataFilename);
        verifyFile = new EnhanceFile(DEFAULT_RAFT_LOG_DIR, verifyFilename);
    }

    private synchronized void append(Entry entry) throws IOException {
        byte[] data = entry.getDataBytes();
        int offset = position.get();
        int length = data.length;
        dataFile.write(ByteBuffer.wrap(data), offset);
        position.getAndAdd(data.length);

        EntryIndex index = new EntryIndex(entry.term(), entry.commitIndex(), offset, length);
        indexFile.write(ByteBuffer.wrap(index.toBytes()),
                (long) cursor.get() * ENTRY_INDEX_LENGTH);

        cursor.getAndIncrement();
        setVerifyCursor(cursor.get());
    }

    private void setVerifyCursor(int cursor) throws IOException {
        ByteBuffer verifyBuffer = ByteBufferUtils.intByteBuffer().putInt(cursor);
        verifyBuffer.rewind();
        verifyFile.write(verifyBuffer, 0);
    }

    public void append(Entry[] entries) throws IOException {
        for(Entry entry : entries) {
            append(entry);
        }
    }

    public Entry lastEntry() throws IOException {
        return getEntryByCommitIndex(cursor.get());
    }

    public Entry getEntryByCommitIndex(int commitIndex) throws IOException {
        assert commitIndex > 0;
        if(commitIndex > cursor.get()) {
            return null;
        }

        EntryIndex index = getEntryIndexByCommitIndex(commitIndex);
        ByteBuffer data = ByteBuffer.allocate(index.length());
        dataFile.read(data, index.offset());
        return Entry.fromDataBytes(index.term(), index.commitIndex(), data.array());
    }

    private EntryIndex getEntryIndexByCommitIndex(int commitIndex) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_INDEX_LENGTH);
        indexFile.read(buffer, (long) (commitIndex - 1) * ENTRY_INDEX_LENGTH);
        return EntryIndex.fromBytes(buffer.array());
    }

    /**
     * 重设游标到 index 位置，相当于删除掉 index 以后的日志条目
     * 保留了 index 所在的日志条目
     * @param index 索引位置
     */
    public synchronized void resetLogCursor(int index) throws IOException {
        cursor.set(index);
        EntryIndex entryIndex = getEntryIndexByCommitIndex(index);
        position.set(entryIndex.offset() + entryIndex.length());
        setVerifyCursor(index);
    }

    /**
     * 获取 commitIndex 在前开后闭区间 (begin, end] 上的所有日志条目
     * @param begin 开始位置
     * @param end 结束位置
     * @return 区间中的所有日志条目
     * @throws IOException IO异常
     */
    public Entry[] getEntriesByRange(int begin, int end) throws IOException {
        assert end > begin && begin > 0;
        Entry[] entries = new Entry[end - begin];

        for (int i = 0; i < entries.length; i++) {
            entries[i] = getEntryByCommitIndex(begin + i + 1);
        }

        return entries;
    }

    public void delete() throws IOException {
        indexFile.delete();
        dataFile.delete();
        verifyFile.delete();
    }
}
