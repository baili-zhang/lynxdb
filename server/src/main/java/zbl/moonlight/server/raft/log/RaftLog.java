package zbl.moonlight.server.raft.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.protocol.Parser;

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
 */
public class RaftLog {
    private static final Logger logger = LogManager.getLogger("BinaryLog");

    private static final String FILE_EXTENSION = ".log";
    private static final String DEFAULT_FOLDER = "/logs";

    private final FileOutputStream dataFileOutputStream;
    private final FileInputStream dataFileInputStream;

    private final FileOutputStream indexFileOutputStream;
    private final FileInputStream indexFileInputStream;

    private long dataFileOffset = 0;
    private long indexFileOffset = 0;

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

    public void append(RaftLogEntry logEntry) throws IOException {
        ByteBuffer dataEntryBuffer = logEntry.serializeData();
        ByteBuffer indexEntryBuffer = logEntry.serializeIndex((int) dataFileOffset, dataEntryBuffer.limit());

        FileChannel dataFileChannel = dataFileOutputStream.getChannel();
        dataFileOffset += dataFileChannel.write(dataEntryBuffer, dataFileOffset);

        FileChannel indexFileChannel = indexFileOutputStream.getChannel();
        indexFileOffset += indexFileChannel.write(indexEntryBuffer, indexFileOffset);
    }

    public void append(RaftLogEntry logEntry, int pos) throws IOException {
        resetPosition(pos);
        append(logEntry);
    }

    /** 重置日志到pos位置，pos位置的日志被保留，后面的日志被覆盖 */
    private void resetPosition(int pos) throws IOException {
        long offset = pos * (long) INDEX_ENTRY_LENGTH;

        if(offset > indexFileOffset) {
            throw new RuntimeException("Index file overflow.");
        }

        if(offset == indexFileOffset) {
            return;
        }

        FileChannel channel = indexFileInputStream.getChannel();
        ByteBuffer lastEntry = ByteBuffer.allocate(INDEX_ENTRY_LENGTH);
        channel.read(lastEntry, offset);
        RaftIndexEntry entry = RaftIndexEntry.parse(lastEntry);

        indexFileOffset = offset;
        dataFileOffset = entry.offset() + entry.length();
    }

    public void appendAll(RaftLogEntry[] entries, int pos) throws IOException {
        resetPosition(pos);
        for (RaftLogEntry entry : entries) {
            append(entry);
        }
    }

    public RaftLogEntry[] readAll() throws IOException {
        int n = (int) indexFileOffset / 4;
        RaftLogEntry[] entries = new RaftLogEntry[n];

        for (int i = 0; i < n; i++) {
            entries[i] = read(i);
        }

        return entries;
    }

    public RaftLogEntry read(int pos) throws IOException {
        FileChannel dataFileChannel = dataFileInputStream.getChannel();
        FileChannel indexFileChannel = indexFileInputStream.getChannel();

        ByteBuffer indexEntryByteBuffer = ByteBuffer.allocate(INDEX_ENTRY_LENGTH);

        indexFileChannel.read(indexEntryByteBuffer, (long) pos * INDEX_ENTRY_LENGTH);
        RaftIndexEntry indexEntry = RaftIndexEntry.parse(indexEntryByteBuffer);

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

    public RaftLogEntry[] readN(int pos, int n) throws IOException {
        RaftLogEntry[] logEntries = new RaftLogEntry[n];

        for (int i = pos; i < n; i++) {
            logEntries[i] = read(i);
        }

        return logEntries;
    }
}
