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

package com.bailizhang.lynxdb.table.lsmtree.level;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import com.bailizhang.lynxdb.table.exception.DeletedException;
import com.bailizhang.lynxdb.table.exception.TimeoutException;
import com.bailizhang.lynxdb.table.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.table.lsmtree.sstable.KeyEntry;
import com.bailizhang.lynxdb.table.lsmtree.sstable.SsTable;
import com.bailizhang.lynxdb.table.schema.Key;

import java.nio.file.Path;
import java.util.*;

public class Level {
    public static final int LEVEL_SSTABLE_COUNT = 10;

    private final LogGroup valueFileGroup;
    private final String parentDir;
    private final Path baseDir;
    private final int levelNo;
    private final Levels parent;
    private final LsmTreeOptions options;

    private LinkedList<SsTable> ssTables = new LinkedList<>();

    public Level(String dir, int level, Levels levels, LogGroup logGroup, LsmTreeOptions lsmOptions) {
        parentDir = dir;
        baseDir = Path.of(dir, String.valueOf(level));
        FileUtils.createDirIfNotExisted(baseDir.toFile());

        levelNo = level;
        parent = levels;
        valueFileGroup = logGroup;
        options = lsmOptions;

        List<String> subs = FileUtils.findSubFiles(baseDir);
        subs.sort(Comparator.comparingInt(NameUtils::id));

        for(String sub : subs) {
            int ssTableNo = NameUtils.id(sub);

            SsTable ssTable = new SsTable(
                    baseDir,
                    ssTableNo,
                    valueFileGroup
            );

            if(ssTables.size() > LEVEL_SSTABLE_COUNT) {
                throw new RuntimeException();
            }

            ssTables.addFirst(ssTable);
        }
    }

    public void merge(MemTable immutable) {
        if(isFull()) {
            mergeToNextLevel();
        }

        createNextSsTable(immutable.all());
        valueFileGroup.clearDeletedEntries();
    }

    public void merge(Level level) {
        if(isFull()) {
            mergeToNextLevel();
        }

        createNextSsTable(level.all());
    }

    public List<KeyEntry> all() {
        int initialCapacity = ((int) Math.pow(10, levelNo)) * options.memTableSize();
        HashMap<Key, KeyEntry> entriesMap = new HashMap<>(initialCapacity);

        // 去重，后面插入的值覆盖前面的值
        ssTables.forEach(ssTable -> ssTable.all(entriesMap));
        List<KeyEntry> keyEntries = new ArrayList<>(entriesMap.values());
        Collections.sort(keyEntries);

        return keyEntries;
    }

    public boolean isFull() {
        return ssTables.size() >= LEVEL_SSTABLE_COUNT;
    }

    public byte[] find(byte[] key) throws DeletedException, TimeoutException {
        for(SsTable ssTable : ssTables) {
            if(ssTable.bloomFilterContains(key)) {
                byte[] value = ssTable.find(key);
                if(value != null) {
                    return value;
                }
            }
        }
        return null;
    }

    /**
     * 使用布隆过滤器判断是否存在 key
     *
     * @param key key
     * @return contains key or not
     */
    public boolean contains(byte[] key) {
        for(SsTable ssTable : ssTables) {
            if(ssTable.bloomFilterContains(key)) {
                return true;
            }
        }

        return false;
    }

    public boolean existKey(byte[] key) throws DeletedException, TimeoutException {
        for(SsTable ssTable : ssTables) {
            if(ssTable.existKey(key)) {
                return true;
            }
        }

        return false;
    }

    public List<Key> rangeNext(
            byte[] beginKey,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys
    ) {
        return range(
                beginKey,
                limit,
                deletedKeys,
                existedKeys,
                null,
                SsTable::rangeNext
        );
    }

    public List<Key> rangeBefore(
            byte[] endKey,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys
    ) {
        return range(
                endKey,
                limit,
                deletedKeys,
                existedKeys,
                Comparator.reverseOrder(),
                SsTable::rangeBefore
        );
    }

    private List<Key> range(
            byte[] baseKey,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys,
            Comparator<Key> comparator,
            RangeOperator operator
    ) {
        PriorityQueue<Key> priorityQueue = new PriorityQueue<>(comparator);

        for(SsTable ssTable : ssTables) {
            List<Key> keys = operator.doRange(
                    ssTable,
                    baseKey,
                    limit,
                    deletedKeys,
                    existedKeys
            );
            priorityQueue.addAll(keys);
        }

        List<Key> range = new ArrayList<>();

        for(int i = 0; i < limit; i ++) {
            Key key = priorityQueue.poll();

            if(key == null) {
                break;
            }

            range.add(key);
        }

        return range;
    }

    private void mergeToNextLevel() {
        int nextLevelNo = levelNo + 1;
        Level nextLevel = parent.get(nextLevelNo);

        if(nextLevel == null) {
            nextLevel = new Level(parentDir, nextLevelNo, parent, valueFileGroup, options);
            parent.put(nextLevelNo, nextLevel);
        }

        nextLevel.merge(this);

        FileUtils.deleteSubs(baseDir);
        ssTables = new LinkedList<>();
    }

    private void createNextSsTable(List<KeyEntry> keyEntries) {
        int nextSsTableNo = ssTables.size();

        SsTable ssTable = SsTable.create(
                baseDir,
                nextSsTableNo,
                levelNo,
                options,
                keyEntries,
                valueFileGroup
        );
        ssTables.addFirst(ssTable);
    }

    @FunctionalInterface
    private interface RangeOperator {
        List<Key> doRange(
                SsTable ssTable,
                byte[] baseKey,
                int limit,
                HashSet<Key> deletedKeys,
                HashSet<Key> existedKeys
        );
    }
}
