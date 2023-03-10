package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.lsmtree.schema.Key;

import java.nio.file.Path;
import java.util.*;

public class LevelTree {
    public final static int LEVEL_BEGIN = 1;

    private final static int EXTRA_DATA_LENGTH = 1;
    private final static String VALUE_GROUP_NAME = "value";

    private final HashMap<Integer, Level> levels = new HashMap<>();
    private final String baseDir;
    private final LogGroup valueFileGroup;
    private final LsmTreeOptions options;

    public LevelTree(String dir, LsmTreeOptions lsmOptions) {
        baseDir = dir;
        options = lsmOptions;

        // 初始化 valueFileGroup
        String valueFileGroupPath = Path.of(baseDir, VALUE_GROUP_NAME).toString();
        LogGroupOptions logOptions = new LogGroupOptions(EXTRA_DATA_LENGTH);

        valueFileGroup = new LogGroup(valueFileGroupPath, logOptions);

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

    public byte[] find(byte[] key) throws DeletedException {
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
    public boolean existKey(byte[] key) throws DeletedException {
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
        int levelNo = LEVEL_BEGIN;
        Level level = levels.get(levelNo);

        PriorityQueue<Key> priorityQueue = new PriorityQueue<>();
        while(level != null) {
            List<Key> keys = level.rangeNext(beginKey, limit, deletedKeys, existedKeys);
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
}
