package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.entry.KeyEntry;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.lsmtree.schema.Key;

import java.nio.file.Path;
import java.util.*;

public class Level {
    public static final int LEVEL_SSTABLE_COUNT = 10;

    private final LogGroup valueFileGroup;
    private final String parentDir;
    private final Path baseDir;
    private final int levelNo;
    private final LevelTree parent;
    private final LsmTreeOptions options;

    private LinkedList<SsTable> ssTables = new LinkedList<>();

    public Level(String dir, int level, LevelTree levelTree, LogGroup logGroup, LsmTreeOptions lsmOptions) {
        parentDir = dir;
        baseDir = Path.of(dir, String.valueOf(level));
        FileUtils.createDirIfNotExisted(baseDir.toFile());

        levelNo = level;
        parent = levelTree;
        valueFileGroup = logGroup;
        options = lsmOptions;

        List<String> subs = FileUtils.findSubFiles(baseDir);
        subs.sort(Comparator.comparingInt(NameUtils::id));

        for(String sub : subs) {
            int id = NameUtils.id(sub);

            Path filePath = Path.of(
                    baseDir.toString(),
                    NameUtils.name(id)
            );

            SsTable ssTable = new SsTable(
                    filePath,
                    levelNo,
                    valueFileGroup,
                    options
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
    }

    public void merge(Level level) {
        if(isFull()) {
            mergeToNextLevel();
        }

        createNextSsTable(level.all());
    }

    public List<KeyEntry> all() {
        HashSet<KeyEntry> entrySet = new HashSet<>();

        // 去重
        ssTables.forEach(ssTable -> ssTable.all(entrySet));
        List<KeyEntry> keyEntries = new ArrayList<>(entrySet);
        Collections.sort(keyEntries);

        return keyEntries;
    }

    public boolean isFull() {
        return ssTables.size() >= LEVEL_SSTABLE_COUNT;
    }

    public byte[] find(byte[] key) throws DeletedException {
        for(SsTable ssTable : ssTables) {
            if(ssTable.contains(key)) {
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
            if(ssTable.contains(key)) {
                return true;
            }
        }

        return false;
    }

    public boolean existKey(byte[] key) throws DeletedException {
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
        Path nextSsTablePath = Path.of(baseDir.toString(), NameUtils.name(nextSsTableNo));
        SsTable ssTable = SsTable.create(
                nextSsTablePath,
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
