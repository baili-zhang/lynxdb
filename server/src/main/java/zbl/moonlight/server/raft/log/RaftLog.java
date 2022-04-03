package zbl.moonlight.server.raft.log;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.protocol.Serializer;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.server.raft.RaftState;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

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

    private static final Configuration config = MdtpServerContext.getInstance().getConfiguration();
    /** 数据文件名 */
    private static final String DATA_FILENAME = config.getHost() + "_" + config.getPort() + "_" + "data";
    private static final String INDEX_FILENAME = config.getHost() + "_" + config.getPort() + "_" + "index";
    private static final String FILE_EXTENSION = ".log";
    private static final String DEFAULT_FOLDER = "/logs";

    private static final int INDEX_ENTRY_LENGTH = 16;
    private static final int TERM_OFFSET = 0;
    private static final int COMMIT_INDEX_OFFSET = 4;
    private static final int OFFSET_OFFSET = 8;
    private static final int LENGTH_OFFSET = 12;

    private final FileOutputStream dataFileOutputStream;
    private final FileInputStream dataFileInputStream;

    private final FileOutputStream indexFileOutputStream;
    private final FileInputStream indexFileInputStream;

    private long dataFileOffset = 0;
    private long indexFileOffset = 0;

    public RaftLog() throws IOException {
        String baseDir = System.getProperty("user.dir");

        Path dir = Path.of(baseDir, DEFAULT_FOLDER);
        if(!dir.toFile().exists()) {
            Files.createDirectories(dir);
        }

        Path dataPath = Path.of(baseDir, DEFAULT_FOLDER, DATA_FILENAME + FILE_EXTENSION);
        File dataFile = dataPath.toFile();
        if (!dataFile.exists()) {
            dataFile.createNewFile();
        }

        dataFileInputStream = new FileInputStream(dataFile);
        dataFileOutputStream = new FileOutputStream(dataFile);

        Path indexPath = Path.of(baseDir, DEFAULT_FOLDER, INDEX_FILENAME + FILE_EXTENSION);
        File indexFile = indexPath.toFile();
        if (!indexFile.exists()) {
            indexFile.createNewFile();
        }

        indexFileInputStream = new FileInputStream(indexFile);
        indexFileOutputStream = new FileOutputStream(indexFile);
    }

    private void append(RaftLogEntry entry) throws IOException {
        ByteBuffer dataEntryBuffer = entry.serialize();

        RaftState raftState = MdtpServerContext.getInstance().getRaftState();
        Serializer serializer = new Serializer(RaftIndexLogSchema.class, false);
        serializer.mapPut(RaftIndexLogSchema.TERM, ByteArrayUtils.fromInt(raftState.getCurrentTerm()));
        serializer.mapPut(RaftIndexLogSchema.COMMIT_INDEX, ByteArrayUtils.fromInt(
                (int) indexFileOffset / INDEX_ENTRY_LENGTH));
        serializer.mapPut(RaftIndexLogSchema.OFFSET, ByteArrayUtils.fromInt((int)dataFileOffset));
        serializer.mapPut(RaftIndexLogSchema.LENGTH, ByteArrayUtils.fromInt(dataEntryBuffer.limit()));
        ByteBuffer indexEntryBuffer = serializer.getByteBuffer();

        FileChannel dataFileChannel = dataFileOutputStream.getChannel();
        dataFileOffset += dataFileChannel.write(dataEntryBuffer, dataFileOffset);

        FileChannel indexFileChannel = indexFileOutputStream.getChannel();
        indexFileOffset += indexFileChannel.write(indexEntryBuffer, indexFileOffset);
    }

    private void resetPosition(int pos) throws IOException {
        long offset = pos * (long) INDEX_ENTRY_LENGTH;

        if(offset > indexFileOffset) {
            throw new RuntimeException("Index file overflow.");
        }

        if(offset == indexFileOffset) {
            return;
        }

        indexFileOffset = offset;

        FileChannel channel = indexFileInputStream.getChannel();
        ByteBuffer lastEntry = ByteBuffer.allocate(INDEX_ENTRY_LENGTH);
        channel.read(lastEntry, indexFileOffset);
        int dataOffset = lastEntry.getInt(OFFSET_OFFSET);
        int dataLength = lastEntry.getInt(LENGTH_OFFSET);

        dataFileOffset = dataOffset + dataLength;
    }

    public void appendAll(int pos, RaftLogEntry[] entries) throws IOException {
        resetPosition(pos);
        for (RaftLogEntry entry : entries) {
            append(entry);
        }
    }

}
