package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * TODO: 1. 应该保证多线程安全, 2. 每条记录添加 CRC 校验，3. 容量 size 检查
 */
public class LogGroup implements Iterable<LogEntry> {
    private static final int DEFAULT_BEGIN_REGION_ID = 1;
    private static final int BEGIN_GLOBAL_LOG_INDEX = 1;

    private final String groupDir;
    private final LogGroupOptions options;

    private final int beginRegionId;
    private int endRegionId;

    private final LinkedList<LogRegion> logRegions = new LinkedList<>();

    public LogGroup(String dir, LogGroupOptions options) {
        groupDir = dir;
        this.options = options;

        File file = new File(groupDir);

        if(!file.exists()) {
            FileUtils.createDir(file);
        }

        if(!file.isDirectory()) {
            throw new RuntimeException(groupDir + " is not a directory");
        }

        String[] filenames = file.list();

        if(filenames != null) {
            Integer[] logRegionIds = Arrays.stream(filenames)
                    .map(NameUtils::id)
                    .toArray(Integer[]::new);

            if(logRegionIds.length != 0) {
                Arrays.sort(logRegionIds);

                int gap = logRegionIds[0];
                for(int i = 0; i < logRegionIds.length; i ++) {
                    if(logRegionIds[i] - i != gap) {
                        throw new RuntimeException("Not found log region, id: " + (i + gap));
                    }
                }

                beginRegionId = logRegionIds[0];
                endRegionId = logRegionIds[logRegionIds.length - 1];

                for(int id = beginRegionId; id <= endRegionId; id ++) {
                    logRegions.add(new LogRegion(id, groupDir, options));
                }

                return;
            }
        }

        beginRegionId = DEFAULT_BEGIN_REGION_ID;
        endRegionId = DEFAULT_BEGIN_REGION_ID;

        LogRegion region = new LogRegion(
                beginRegionId,
                groupDir,
                options
        );

        region.globalIdxBegin(BEGIN_GLOBAL_LOG_INDEX);
        region.globalIdxEnd(BEGIN_GLOBAL_LOG_INDEX - 1);

        logRegions.add(region);
    }

    public LogEntry find(int globalIndex) {
        LinkedList<LogEntry> logEntries = range(globalIndex, globalIndex);
        return logEntries.isEmpty() ? null : logEntries.getFirst();
    }

    /**
     * [beginGlobalIndex, globalEndIndex]
     *
     * @param beginGlobalIndex begin global index
     * @param endGlobalIndex end global index
     */
    public LinkedList<LogEntry> range(int beginGlobalIndex, int endGlobalIndex) {
        LinkedList<LogEntry> entries = new LinkedList<>();

        for(LogRegion region : logRegions) {
            if(beginGlobalIndex > region.globalIdxEnd()) {
                continue;
            }

            if(endGlobalIndex < region.globalIdxBegin()) {
                break;
            }

            int begin = Math.max(region.globalIdxBegin(), beginGlobalIndex);
            int end = Math.min(region.globalIdxEnd(), endGlobalIndex);
            for(int globalIndex = begin; globalIndex <= end; globalIndex ++) {
                LogEntry entry = region.readEntry(globalIndex);
                entries.add(entry);
            }
        }

        return entries;
    }

    /**
     * 不会删除 globalIndex 这条记录
     *
     * @param globalIndex global index
     */
    public void deleteOldThan(int globalIndex) {
        while(true) {
            LogRegion logRegion = logRegions.getFirst();

            if(globalIndex <= logRegion.globalIdxEnd()) {
                break;
            }

            logRegions.removeFirst();
            logRegion.delete();
        }
    }

    public int maxGlobalIndex() {
        return lastRegion().globalIdxEnd();
    }

    public byte[] lastExtraData() {
        return lastRegion().lastExtraData();
    }

    public void delete() {
        FileUtils.delete(Path.of(groupDir));
    }

    /**
     * 需要保证多线程同步
     *
     * TODO：加读写锁同步
     *
     * @param extraData extra data
     * @param data data
     * @return global index
     */
    public synchronized int append(byte[] extraData, byte[] data) {
        LogRegion region = lastRegion();
        int globalIndex = region.append(extraData, data);

        if(region.isFull()) {
            createNextRegion();
        }

        return globalIndex;
    }

    public int append(byte[] extraData, BytesListConvertible convertible) {
        byte[] data = convertible.toBytesList().toBytes();
        return append(extraData, data);
    }

    @Override
    public Iterator<LogEntry> iterator() {
        return new LogGroupIterator(this);
    }

    private LogRegion getRegion(int id) {
        return logRegions.get(id - beginRegionId);
    }

    private LogRegion beginRegion() {
        return logRegions.get(0);
    }

    private LogRegion lastRegion() {
        return logRegions.get(logRegions.size() - 1);
    }

    private void createNextRegion() {
        LogRegion region = new LogRegion(
                ++ endRegionId,
                groupDir,
                options
        );

        region.globalIdxBegin(maxGlobalIndex() + 1);
        region.globalIdxEnd(maxGlobalIndex());

        logRegions.add(region);
    }

    private static class LogGroupIterator implements Iterator<LogEntry> {
        private final LogGroup logGroup;

        private int regionId;
        private int globalIndex;

        public LogGroupIterator(LogGroup logGroup) {
            this.logGroup = logGroup;
            LogRegion region = logGroup.beginRegion();
            regionId = region.id();
            globalIndex = region.globalIdxBegin();
        }

        @Override
        public boolean hasNext() {
            return globalIndex <= logGroup.maxGlobalIndex();
        }

        @Override
        public LogEntry next() {
            if(!hasNext()) {
                return null;
            }

            LogEntry logEntry;
            LogRegion region = logGroup.getRegion(regionId);
            if (globalIndex > region.globalIdxEnd()) {
                region = logGroup.getRegion(++regionId);
            }
            logEntry = region.readEntry(globalIndex);

            globalIndex ++;
            return logEntry;
        }
    }
}
