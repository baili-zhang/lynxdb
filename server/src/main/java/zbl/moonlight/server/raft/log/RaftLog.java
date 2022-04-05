package zbl.moonlight.server.raft.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.protocol.Parser;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static zbl.moonlight.server.raft.log.RaftIndexEntry.*;

/**
 * 二进制日志（给Raft协议用的）：
 *  1.数据文件
 *  2.索引文件
 *
 * 数据文件格式：
 *  |方法    |键     |值      |
 *  |---    |---    |---    |
 *  |method |key    |value  |
 *
 * 索引文件格式：
 *  |任期    |日志的提交索引  |数据文件中的偏移量  |日志条目的长度   |
 *  |---    |---          |---              |---           |
 *  |term   |commit_index |offset           |length        |
 *
 * cursor:
 *   指向下一个写入的位置
 *
 *  |log index   |0      |1(cursor)|2      |3      |4      |
 *  |---         |---    |---      |---    |---    |---    |
 *  |log entry   |(entry)|(empty)  |(empty)|(empty)|(empty)|
 */
public class RaftLog {
    private static final Logger logger = LogManager.getLogger("BinaryLog");

    private static final String FILE_EXTENSION = ".log";
    private static final String DEFAULT_FOLDER = "/logs";

    private final FileOutputStream dataFileOutputStream;
    private final FileInputStream dataFileInputStream;

    private final FileOutputStream indexFileOutputStream;
    private final FileInputStream indexFileInputStream;

    /** 逻辑游标 */
    private int cursor = 0;

    public RaftLog(String dataFilename, String indexFilename) throws IOException {
        String baseDir = System.getProperty("user.dir");

        Path dir = Path.of(baseDir, DEFAULT_FOLDER);
        if(!dir.toFile().exists()) {
            Files.createDirectories(dir);
        }

        Path dataPath = Path.of(baseDir, DEFAULT_FOLDER, dataFilename + FILE_EXTENSION);
        File dataFile = dataPath.toFile();
        if (!dataFile.exists()) {
            dataFile.createNewFile();
        }

        dataFileInputStream = new FileInputStream(dataFile);
        dataFileOutputStream = new FileOutputStream(dataFile);

        Path indexPath = Path.of(baseDir, DEFAULT_FOLDER, indexFilename + FILE_EXTENSION);
        File indexFile = indexPath.toFile();
        if (!indexFile.exists()) {
            indexFile.createNewFile();
        }

        indexFileInputStream = new FileInputStream(indexFile);
        indexFileOutputStream = new FileOutputStream(indexFile);
    }

    /** 从磁盘的文件中恢复数据 */
    public void recover() throws IOException {
        EventBus eventBus = MdtpServerContext.getInstance().getEventBus();
        RaftLogEntry logEntry;

        while ((logEntry = read(cursor)) != null) {
            /* 发送事件给事件总线 */
            eventBus.offer(new Event(EventType.ENGINE_REQUEST, logEntry));
            cursor ++;
        }
    }

    public void append(RaftLogEntry logEntry) throws IOException {
        int dataFileOffset = 0;
        /* 获取最后一个index log entry */
        RaftIndexEntry lastIndexEntry = RaftIndexEntry.read(indexFileInputStream.getChannel(), cursor - 1);

        if(lastIndexEntry != null) {
            dataFileOffset = lastIndexEntry.offset() + lastIndexEntry.length();
        }

        ByteBuffer dataEntryBuffer = logEntry.serializeData();
        ByteBuffer indexEntryBuffer = logEntry.serializeIndex(
                dataFileOffset, dataEntryBuffer.limit());

        /* 写入data entry */
        FileChannel dataFileChannel = dataFileOutputStream.getChannel();
        dataFileChannel.write(dataEntryBuffer, dataFileOffset);

        RaftIndexEntry.write(indexFileOutputStream.getChannel(), indexEntryBuffer, cursor ++);
    }

    public void append(RaftLogEntry logEntry, int cursor) throws IOException {
        resetCursor(cursor);
        append(logEntry);
    }

    /** 重置游标 */
    private void resetCursor(int newCursor) {
        if(newCursor > cursor) {
            throw new RuntimeException("Index file overflow.");
        }
        cursor = newCursor;
    }

    public void appendAll(RaftLogEntry[] entries, int cursor) throws IOException {
        resetCursor(cursor);
        for (RaftLogEntry entry : entries) {
            append(entry);
        }
    }

    public RaftLogEntry read(int cursor) throws IOException {
        FileChannel dataFileChannel = dataFileInputStream.getChannel();
        FileChannel indexFileChannel = indexFileInputStream.getChannel();

        RaftIndexEntry indexEntry = RaftIndexEntry.read(indexFileChannel, cursor);

        if(indexEntry == null) return null;

        ByteBuffer entryBuffer = ByteBuffer.allocate(indexEntry.length());
        dataFileChannel.read(entryBuffer, indexEntry.offset());

        Parser parser = new Parser(RaftDataLogSchema.class);
        parser.setByteBuffer(entryBuffer);
        parser.parse();
        byte method = parser.mapGet(RaftDataLogSchema.METHOD)[0];
        byte[] key = parser.mapGet(RaftDataLogSchema.KEY);
        byte[] value = parser.mapGet(RaftDataLogSchema.VALUE);

        return new RaftLogEntry(indexEntry.term(), indexEntry.commitIndex(), method, key, value);
    }

    public RaftLogEntry[] readN(int cursor, int n) throws IOException {
        RaftLogEntry[] logEntries = new RaftLogEntry[n];

        for (int i = 0; i < n; i++) {
            logEntries[i] = read(cursor + i);
        }

        return logEntries;
    }
}
