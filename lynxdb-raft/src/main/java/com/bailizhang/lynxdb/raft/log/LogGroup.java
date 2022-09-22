package com.bailizhang.lynxdb.raft.log;

import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.raft.log.entry.LogEntryMethod;
import com.bailizhang.lynxdb.raft.log.entry.LogEntryType;
import com.bailizhang.lynxdb.raft.log.entry.LogEntryValid;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * TODO: 日志 Region 压缩以后再做
 */
public class LogGroup {
    private static final String groupDir = RaftConfiguration.getInstance().logDir();

    private static final long DEFAULT_FILE_THRESHOLD = 4 * 1024 * 1024;
    private static final int BEGIN_ID = 1;
    private static final int BEGIN_INDEX = 1;

    private final List<Integer> indexList;

    public LogGroup() {
        File file = new File(groupDir);

        if(!file.exists()) {
            FileUtils.createDir(file);
        }

        if(!file.isDirectory()) {
            throw new RuntimeException(groupDir + " is not a directory");
        }

        String[] filenames = file.list();

        if(filenames == null) {
            indexList = new ArrayList<>();
            return;
        }

        List<Integer> list = Arrays.stream(filenames)
                .map(filename -> {
                    String[] temp = filename.split("\\.");
                    return Integer.valueOf(temp[0]);
                })
                .toList();

        indexList = new ArrayList<>(list);
        indexList.sort(Integer::compareTo);
    }

    public void appendKvDelete(int term, BytesListConvertible convertible) {
        append(LogEntryMethod.DELETE, LogEntryType.KV_STORE, term, convertible);
    }

    public void appendKvSet(int term, BytesListConvertible convertible) {
        append(LogEntryMethod.SET, LogEntryType.KV_STORE, term, convertible);
    }

    public void appendTableDelete(int term, BytesListConvertible convertible) {
        append(LogEntryMethod.DELETE, LogEntryType.TABLE, term, convertible);
    }

    public void appendTableSet(int term, BytesListConvertible convertible) {
        append(LogEntryMethod.SET, LogEntryType.TABLE, term, convertible);
    }

    /**
     * [begin, end]
     *
     * @param begin begin index
     * @param end end index
     */
    public LinkedList<LogEntry> range(int begin, int end) {
        LinkedList<LogEntry> entries = new LinkedList<>();

        for(int i : indexList) {
            LogRegion region = new LogRegion(i);

            if(begin > region.end()) {
                continue;
            }

            int minEnd = Math.min(region.end(), end);
            for(int j = begin; j <= minEnd; j ++) {
                LogEntry entry = region.readEntry(j);
                entries.add(entry);
            }

            if(end <= region.end()) {
                break;
            }
        }

        return entries;
    }

    public void delete() {
        FileUtils.delete(Path.of(groupDir));
    }

    private void append(byte method, byte type, int term, BytesListConvertible convertible) {
        LogRegion region = lastRegion();

        if(region.length() >= DEFAULT_FILE_THRESHOLD) {
            region = nextRegion();
        }

        long current = region.length();
        byte[] data = convertible.toBytesList().toBytes();

        LogEntry entry = new LogEntry(method, LogEntryValid.VALID, type, current, term, data);
        region.append(entry);
    }

    private LogRegion lastRegion() {
        if(indexList.isEmpty()) {
            return LogRegion.create(BEGIN_ID, BEGIN_INDEX);
        }

        Integer id = indexList.get(indexList.size() - 1);
        return new LogRegion(id);
    }

    private LogRegion nextRegion() {
        Integer id = indexList.get(indexList.size() - 1);
        indexList.add(++ id);
        return new LogRegion(id);
    }

    public LogRegion lastEntry() {
        Integer id = indexList.get(indexList.size() - 1);
        return new LogRegion(id);
    }
}
