package zbl.moonlight.core.raft.log;

import zbl.moonlight.core.raft.request.Entry;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftLog {
    public static final String DEFAULT_RAFT_INDEX_LOG_PATH
            = System.getProperty("user.dir") + "/logs/raft_log.index";
    public static final String DEFAULT_RAFT_DATA_LOG_PATH
            = System.getProperty("user.dir") + "/logs/raft_log.data";

    private final EnhanceFile indexFile;
    private final EnhanceFile dataFile;

    private AtomicInteger cursor = new AtomicInteger(0);

    public RaftLog() throws IOException {
        this(DEFAULT_RAFT_INDEX_LOG_PATH, DEFAULT_RAFT_DATA_LOG_PATH);
    }

    public RaftLog(String indexFilePath, String dataFilePath) throws IOException {
        indexFile = new EnhanceFile(indexFilePath);
        dataFile = new EnhanceFile(dataFilePath);
    }

    public void append(Entry[] entries) {

    }

    public Entry lastEntry() {
        return null;
    }

    public Entry getEntryByCommitIndex(int commitIndex) {
        return null;
    }

    /**
     * 重设游标到 index 位置，相当于删除掉 index 位置及以后的日志条目
     * @param index
     */
    public void resetLogCursor(int index) {

    }

    public Entry[] getEntriesByRange(int begin, int end) {
        return null;
    }
}
