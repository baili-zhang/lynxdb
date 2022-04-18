package zbl.moonlight.server.raft.log;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.protocol.Parser;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.core.utils.ByteBufferUtils;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 二进制日志（给Raft协议用的）：
 *  1.数据文件
 *  2.索引文件
 *  3.验证文件
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
 *
 *  验证文件：
 *  |有效的最大索引条目数 |
 *  |int              |
 */
public class RaftLog {
    private static final Logger logger = LogManager.getLogger("RaftLog");

    private static final String FILE_EXTENSION = ".log";
    private static final String DEFAULT_FOLDER = "/logs";

    private final FileOutputStream dataFileOutputStream;
    private final FileInputStream dataFileInputStream;

    private final FileOutputStream indexFileOutputStream;
    private final FileInputStream indexFileInputStream;

    private final FileOutputStream verifyFileOutputStream;
    private final FileInputStream verifyFileInputStream;

    /** 逻辑游标 */
    @Getter
    private int cursor = 0;

    public RaftLog(String dataFilename, String indexFilename, String verifyFilename) throws IOException {
        String baseDir = System.getProperty("user.dir");

        Path dir = Path.of(baseDir, DEFAULT_FOLDER);
        if(!dir.toFile().exists()) {
            Files.createDirectories(dir);
        }

        Path dataPath = Path.of(baseDir, DEFAULT_FOLDER, dataFilename + FILE_EXTENSION);
        File dataFile = createIfNotExisted(dataPath);

        dataFileInputStream = new FileInputStream(dataFile);
        dataFileOutputStream = new FileOutputStream(dataFile, true);

        Path indexPath = Path.of(baseDir, DEFAULT_FOLDER, indexFilename + FILE_EXTENSION);
        File indexFile = createIfNotExisted(indexPath);

        indexFileInputStream = new FileInputStream(indexFile);
        indexFileOutputStream = new FileOutputStream(indexFile, true);

        Path verifyPath = Path.of(baseDir, DEFAULT_FOLDER, verifyFilename + FILE_EXTENSION);
        boolean verifyFileExisted = verifyPath.toFile().exists();
        File verifyFile = createIfNotExisted(verifyPath);

        verifyFileInputStream = new FileInputStream(verifyFile);
        verifyFileOutputStream = new FileOutputStream(verifyFile, true);

        if(verifyFileExisted) {
            byte[] bytes = verifyFileInputStream.readAllBytes();
            assert bytes.length == 4;
            cursor = ByteArrayUtils.toInt(bytes);
        } else {
            writeCursor(0);
        }
        logger.info("Max entry size is: {}", cursor);
    }

    private File createIfNotExisted(Path path) throws IOException {
        File file = path.toFile();
        if (!file.exists()) {
            file.createNewFile();
        }
        return file;
    }

    /** 从磁盘的文件中恢复数据 */
    public void recover() throws IOException {
        EventBus eventBus = MdtpServerContext.getInstance().getEventBus();
        RaftLogEntry logEntry;
        int i = 0;

        while (i < cursor) {
            if((logEntry = read(i)) == null) {
                throw new RuntimeException("The raft log is incomplete");
            }
            /* 发送事件给事件总线 */
            eventBus.offer(new Event(EventType.LOG_RECOVER, logEntry));
            i ++;
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
        writeCursor(cursor);
    }

    public void writeCursor(int cursor) throws IOException {
        verifyFileOutputStream.getChannel().write(ByteBuffer.wrap(ByteArrayUtils.fromInt(cursor)), 0);
    }

    public void append(RaftLogEntry logEntry, int cursor) throws IOException {
        resetCursor(cursor);
        append(logEntry);
    }

    /** 重置游标 */
    private void resetCursor(int newCursor) throws IOException {
        if(newCursor > cursor) {
            throw new RuntimeException("Index file overflow.");
        }
        cursor = newCursor;
        writeCursor(cursor);
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

    public void close() throws IOException {
        dataFileInputStream.close();
        dataFileOutputStream.close();
        indexFileInputStream.close();
        indexFileOutputStream.close();
        verifyFileInputStream.close();
        verifyFileOutputStream.close();
    }
}
