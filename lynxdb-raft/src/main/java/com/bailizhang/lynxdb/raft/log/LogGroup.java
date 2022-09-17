package com.bailizhang.lynxdb.raft.log;

import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.raft.log.entry.LogEntryMethod;
import com.bailizhang.lynxdb.raft.log.entry.LogEntryType;
import com.bailizhang.lynxdb.raft.log.entry.LogEntryValid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

        if(!file.isDirectory()) {
            throw new RuntimeException(groupDir + " is not a directory");
        }

        String[] filenames = file.list();

        if(filenames == null) {
            indexList = new ArrayList<>();
            return;
        }

        indexList = Arrays.stream(filenames)
                .map(filename -> {
                    String[] temp = filename.split("\\.");
                    return Integer.valueOf(temp[0]);
                })
                .toList();

        indexList.sort(Integer::compareTo);
    }

    public void appendKvDelete(BytesListConvertible convertible) {
        append(LogEntryMethod.DELETE, LogEntryType.KV_STORE, convertible);
    }

    public void appendKvSet(BytesListConvertible convertible) {
        append(LogEntryMethod.SET, LogEntryType.KV_STORE, convertible);
    }

    public void appendTableDelete(BytesListConvertible convertible) {
        append(LogEntryMethod.DELETE, LogEntryType.TABLE, convertible);
    }

    public void appendTableSet(BytesListConvertible convertible) {
        append(LogEntryMethod.SET, LogEntryType.TABLE, convertible);
    }

    private void append(byte method, byte type, BytesListConvertible convertible) {
        LogRegion region = lastRegion();

        if(region.length() >= DEFAULT_FILE_THRESHOLD) {
            region = nextRegion();
        }

        long current = region.length();
        byte[] data = convertible.toBytesList().toBytes();

        LogEntry entry = new LogEntry(method, LogEntryValid.VALID, type, current, data);
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
}
