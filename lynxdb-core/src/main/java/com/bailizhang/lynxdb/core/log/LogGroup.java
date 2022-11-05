package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.FileUtils;

import java.io.File;
import java.nio.file.Path;
import java.util.*;


public class LogGroup {
    private static final long DEFAULT_FILE_THRESHOLD = 4 * 1024 * 1024;
    private static final int DEFAULT_BEGIN_REGION_ID = 1;
    private static final int BEGIN_GLOBAL_LOG_INDEX = 1;

    private final String groupDir;
    private final LogOptions options;

    private final int beginRegionId;
    private int endRegionId;

    private final List<LogRegion> logRegions = new ArrayList<>();

    public LogGroup(String dir, LogOptions options) {
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
                    .map(filename -> {
                        if(!filename.endsWith(FileUtils.LOG_SUFFIX)) {
                            return null;
                        }

                        String name = filename.replace(FileUtils.LOG_SUFFIX, "");

                        try {
                            return Integer.parseInt(name);
                        } catch (Exception ignore) {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
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
                    logRegions.add(LogRegion.open(id, groupDir, options));
                }

                return;
            }
        }

        beginRegionId = DEFAULT_BEGIN_REGION_ID;
        endRegionId = DEFAULT_BEGIN_REGION_ID;

        LogRegion region = LogRegion.create(
                beginRegionId,
                BEGIN_GLOBAL_LOG_INDEX,
                groupDir,
                options
        );

        logRegions.add(region);
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
            if(beginGlobalIndex > region.globalIndexEnd()) {
                continue;
            }

            if(endGlobalIndex < region.globalIndexBegin()) {
                break;
            }

            int begin = Math.max(region.globalIndexBegin(), beginGlobalIndex);
            int end = Math.min(region.globalIndexEnd(), endGlobalIndex);
            for(int globalIndex = begin; globalIndex <= end; globalIndex ++) {
                LogEntry entry = region.readEntry(globalIndex);
                entries.add(entry);
            }
        }

        return entries;
    }

    public int maxGlobalIndex() {
        return lastRegion().globalIndexEnd();
    }

    public byte[] lastLogExtraData() {
        return lastRegion().extraData();
    }

    public void delete() {
        FileUtils.delete(Path.of(groupDir));
    }

    public int append(byte[] extraData, byte[] data) {
        LogRegion region = lastRegion();

        if(region.isFull() || region.length() >= DEFAULT_FILE_THRESHOLD) {
            region = createNextRegion();
        }

        return region.append(extraData, data);
    }

    public void append(byte[] extraData, BytesListConvertible convertible) {
        byte[] data = convertible.toBytesList().toBytes();
        append(extraData, data);
    }

    private LogRegion lastRegion() {
        return logRegions.get(logRegions.size() - 1);
    }

    private LogRegion createNextRegion() {
        LogRegion region = LogRegion.create(
                ++ endRegionId,
                maxGlobalIndex() + 1,
                groupDir,
                options
        );

        logRegions.add(region);
        return region;
    }
}
