package com.bailizhang.lynxdb.table.lsmtree.level;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import com.bailizhang.lynxdb.table.exception.DeletedException;
import com.bailizhang.lynxdb.table.exception.TimeoutException;
import com.bailizhang.lynxdb.table.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.table.schema.Key;

import java.util.*;

public class Levels {
    public final static int LEVEL_BEGIN = 1;

    private final HashMap<Integer, Level> levels = new HashMap<>();
    private final String baseDir;
    private final LogGroup valueFileGroup;
    private final LsmTreeOptions options;

    public Levels(String dir, LogGroup valueLogGroup, LsmTreeOptions lsmOptions) {
        baseDir = dir;
        options = lsmOptions;
        valueFileGroup = valueLogGroup;

        List<String> subDirs = FileUtils.findSubDirs(baseDir);
        for(String subDir : subDirs) {
            int levelNo;

            try {
                levelNo = Integer.parseInt(subDir);
            } catch (NumberFormatException ignore) {
                continue;
            }

            Level level = new Level(baseDir, levelNo, this, valueFileGroup, options);

            levels.put(levelNo, level);
        }
    }

    public byte[] find(byte[] key) throws DeletedException, TimeoutException {
        int levelNo = LEVEL_BEGIN;
        Level level = levels.get(levelNo);

        while(level != null) {
            if(level.contains(key)) {
                byte[] value = level.find(key);
                if(value != null) {
                    return value;
                }
            }

            level = levels.get(++ levelNo);
        }

        return null;
    }

    public void merge(MemTable immutable) {
        if(immutable == null) {
            return;
        }

        Level level = levels.get(LEVEL_BEGIN);

        if(level == null) {
            level = new Level(baseDir, LEVEL_BEGIN, this, valueFileGroup, options);
            levels.put(LEVEL_BEGIN, level);
        }

        level.merge(immutable);
    }

    Level get(int levelNo) {
        return levels.get(levelNo);
    }

    void put(int levelNo, Level level) {
        levels.put(levelNo, level);
    }

    /**
     * 是否存在 key
     *
     * @param key key
     * @return is existed or not
     */
    public boolean existKey(byte[] key) throws DeletedException, TimeoutException {
        int levelNo = LEVEL_BEGIN;
        Level level = levels.get(levelNo);

        while(level != null) {
            if(level.contains(key)) {
                boolean isExisted = level.existKey(key);
                if(isExisted) {
                    return true;
                }
            }

            level = levels.get(++ levelNo);
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
                Level::rangeNext
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
                Level::rangeBefore
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
        int levelNo = LEVEL_BEGIN;
        Level level = levels.get(levelNo);

        PriorityQueue<Key> priorityQueue = new PriorityQueue<>(comparator);
        while(level != null) {
            List<Key> keys = operator.doRange(level, baseKey, limit, deletedKeys, existedKeys);
            priorityQueue.addAll(keys);

            level = levels.get(++ levelNo);
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

    @FunctionalInterface
    private interface RangeOperator {
        List<Key> doRange(
                Level level,
                byte[] baseKey,
                int limit,
                HashSet<Key> deletedKeys,
                HashSet<Key> existedKeys
        );
    }
}
