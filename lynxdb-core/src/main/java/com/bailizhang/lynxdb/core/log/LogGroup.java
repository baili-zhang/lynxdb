/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * TODO: 1. 应该保证多线程安全
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

    public LogEntry findEntry(int globalIndex) {
        LinkedList<LogEntry> logEntries = range(globalIndex, globalIndex);
        return logEntries.isEmpty() ? null : logEntries.getFirst();
    }

    public int appendEntry(byte[] data) {
        return appendEntry(BufferUtils.toBuffers(data));
    }

    public synchronized int appendEntry(ByteBuffer[] data) {
        LogRegion region = lastRegion();
        int globalIdx = region.appendEntry(data);

        if(region.isFull()) {
            createNextRegion();
        }

        return globalIdx;
    }

    public synchronized void removeEntry(int globalIdx) {
        for(LogRegion region : logRegions) {
            int globalIdxBegin = region.globalIdxBegin();
            int globalIdxEnd = region.globalIdxEnd();

            if(globalIdxEnd < globalIdxBegin || globalIdx > globalIdxEnd) {
                continue;
            }

            region.removeEntry(globalIdx);
            break;
        }
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

    public int maxGlobalIdx() {
        return lastRegion().globalIdxEnd();
    }

    public synchronized void clearDeletedEntries() {
        LinkedList<LogRegion> newLogRegions = new LinkedList<>();

        while (!logRegions.isEmpty()) {
            LogRegion region = logRegions.removeFirst();
            var entries = region.aliveEntries();
            if(entries == null) {
                newLogRegions.add(region);
                continue;
            }

            int id = region.id();
            region.delete();

            LogRegion newRegion = new LogRegion(id, groupDir, options);
            for(var entry : entries) {
                newRegion.appendEntry(entry.left(), entry.right());
            }

            newLogRegions.add(newRegion);
        }

        logRegions.addAll(newLogRegions);
    }

    public void delete() {
        FileUtils.delete(Path.of(groupDir));
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

        region.globalIdxBegin(maxGlobalIdx() + 1);
        region.globalIdxEnd(maxGlobalIdx());

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
            return globalIndex <= logGroup.maxGlobalIdx();
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
